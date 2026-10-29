//! AWS EKS backend — moto-equivalent (rest-json).
//!
//! Mirrors moto's EKS for the suite-covered surface: clusters (with
//! defaults for version/platform/logging/identity), managed nodegroups,
//! fargate profiles, addons, access entries, pod identity associations,
//! identity provider configs, updates, insights, EKS Anywhere
//! subscriptions, capabilities, and resource tagging. The pytest suites
//! (tests/compatibility/test_eks_compat.py, tests/moto_impl/
//! test_eks_lifecycle.py) are the contract.

use crate::store::{self, ScopeKey};
use crate::{Request, Response};
use once_cell::sync::Lazy;
use parking_lot::RwLock;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::Arc;
use uuid::Uuid;

const ACTIVE_STATUS: &str = "ACTIVE";

static DEFAULT_KUBERNETES_NETWORK_CONFIG: Lazy<Value> =
    Lazy::new(|| json!({ "serviceIpv4Cidr": "172.20.0.0/16" }));
const DEFAULT_KUBERNETES_VERSION: &str = "1.19";
const DEFAULT_PLATFORM_VERSION: &str = "eks.4";
static DEFAULT_LOGGING: Lazy<Value> = Lazy::new(|| {
    json!({
        "clusterLogging": [
            {
                "types": ["api", "audit", "authenticator", "controllerManager", "scheduler"],
                "enabled": false,
            }
        ]
    })
});

const DEFAULT_AMI_TYPE: &str = "AL2_x86_64";
const DEFAULT_CAPACITY_TYPE: &str = "ON_DEMAND";
const DEFAULT_RELEASE_VERSION: &str = "1.19.8-20210414";
static DEFAULT_REMOTE_ACCESS: Lazy<Value> = Lazy::new(|| json!({ "ec2SshKey": "eksKeypair" }));
static DEFAULT_SCALING_CONFIG: Lazy<Value> =
    Lazy::new(|| json!({ "minSize": 2, "maxSize": 2, "desiredSize": 2 }));

const CLUSTER_IN_USE_MSG: &str = "Cluster has nodegroups attached";

static ADDON_VERSIONS: Lazy<Value> = Lazy::new(|| {
    json!([
        {
            "addonName": "vpc-cni",
            "type": "networking",
            "addonVersions": [{
                "addonVersion": "v1.12.0-eksbuild.1",
                "architecture": ["amd64", "arm64"],
                "compatibilities": [{
                    "clusterVersion": "1.27",
                    "platformVersions": ["*"],
                    "defaultVersion": true,
                }],
            }],
            "publisher": "eks",
            "owner": "aws",
        },
        {
            "addonName": "coredns",
            "type": "networking",
            "addonVersions": [{
                "addonVersion": "v1.9.3-eksbuild.2",
                "architecture": ["amd64", "arm64"],
                "compatibilities": [{
                    "clusterVersion": "1.27",
                    "platformVersions": ["*"],
                    "defaultVersion": true,
                }],
            }],
            "publisher": "eks",
            "owner": "aws",
        },
        {
            "addonName": "kube-proxy",
            "type": "networking",
            "addonVersions": [{
                "addonVersion": "v1.27.1-eksbuild.1",
                "architecture": ["amd64", "arm64"],
                "compatibilities": [{
                    "clusterVersion": "1.27",
                    "platformVersions": ["*"],
                    "defaultVersion": true,
                }],
            }],
            "publisher": "eks",
            "owner": "aws",
        },
        {
            "addonName": "aws-ebs-csi-driver",
            "type": "storage",
            "addonVersions": [{
                "addonVersion": "v1.19.0-eksbuild.1",
                "architecture": ["amd64", "arm64"],
                "compatibilities": [{
                    "clusterVersion": "1.27",
                    "platformVersions": ["*"],
                    "defaultVersion": true,
                }],
            }],
            "publisher": "eks",
            "owner": "aws",
        },
    ])
});

static ACCESS_POLICIES: Lazy<Value> = Lazy::new(|| {
    json!([
        {
            "name": "AmazonEKSClusterAdminPolicy",
            "arn": "arn:aws:eks::aws:cluster-access-policy/AmazonEKSClusterAdminPolicy",
        },
        {
            "name": "AmazonEKSAdminPolicy",
            "arn": "arn:aws:eks::aws:cluster-access-policy/AmazonEKSAdminPolicy",
        },
        {
            "name": "AmazonEKSEditPolicy",
            "arn": "arn:aws:eks::aws:cluster-access-policy/AmazonEKSEditPolicy",
        },
        {
            "name": "AmazonEKSViewPolicy",
            "arn": "arn:aws:eks::aws:cluster-access-policy/AmazonEKSViewPolicy",
        },
        {
            "name": "AmazonEKSAdminViewPolicy",
            "arn": "arn:aws:eks::aws:cluster-access-policy/AmazonEKSAdminViewPolicy",
        },
    ])
});

static CLUSTER_VERSIONS: Lazy<Value> = Lazy::new(|| {
    json!([
        {
            "clusterVersion": "1.31",
            "clusterType": "eks",
            "defaultPlatformVersion": "eks.14",
            "defaultVersion": true,
            "endOfStandardSupportDate": null,
            "endOfExtendedSupportDate": null,
            "status": "standard-support",
        },
        {
            "clusterVersion": "1.30",
            "clusterType": "eks",
            "defaultPlatformVersion": "eks.18",
            "defaultVersion": false,
            "endOfStandardSupportDate": null,
            "endOfExtendedSupportDate": null,
            "status": "standard-support",
        },
        {
            "clusterVersion": "1.29",
            "clusterType": "eks",
            "defaultPlatformVersion": "eks.19",
            "defaultVersion": false,
            "endOfStandardSupportDate": null,
            "endOfExtendedSupportDate": null,
            "status": "standard-support",
        },
        {
            "clusterVersion": "1.28",
            "clusterType": "eks",
            "defaultPlatformVersion": "eks.22",
            "defaultVersion": false,
            "endOfStandardSupportDate": null,
            "endOfExtendedSupportDate": null,
            "status": "extended-support",
        },
        {
            "clusterVersion": "1.27",
            "clusterType": "eks",
            "defaultPlatformVersion": "eks.24",
            "defaultVersion": false,
            "endOfStandardSupportDate": null,
            "endOfExtendedSupportDate": null,
            "status": "extended-support",
        },
    ])
});

type Stores = Arc<RwLock<std::collections::HashMap<ScopeKey, EksStore>>>;

fn stores() -> Stores {
    store::slot::<EksStore>("eks")
}

/// A cluster plus everything nested under it.
#[derive(Default)]
struct ClusterRecord {
    cluster: Value,
    nodegroups: BTreeMap<String, Value>,
    fargate_profiles: BTreeMap<String, Value>,
    addons: BTreeMap<String, Value>,
    /// principal arn -> access entry
    access_entries: BTreeMap<String, Value>,
    /// association id -> association
    pod_identity_associations: BTreeMap<String, Value>,
    /// config name -> identity provider config
    identity_provider_configs: BTreeMap<String, Value>,
    capabilities: BTreeMap<String, Value>,
}

/// Per-(account, region) in-memory state for EKS.
#[derive(Default)]
struct EksStore {
    clusters: BTreeMap<String, ClusterRecord>,
    /// subscription id -> subscription
    eks_anywhere_subscriptions: BTreeMap<String, Value>,
    /// cluster name -> refresh records (newest first)
    insights_refreshes: BTreeMap<String, Vec<Value>>,
}

fn now() -> f64 {
    chrono::Utc::now().timestamp_millis() as f64 / 1000.0
}

fn arn_prefix(req: &Request) -> String {
    format!("arn:aws:eks:{}:{}", req.region, req.account)
}

fn issuer_for(req: &Request) -> String {
    let id = &Uuid::new_v4().simple().to_string()[..10];
    format!("https://oidc.eks.{}.amazonaws.com/id/{}", req.region, id)
}

fn endpoint_for(req: &Request) -> String {
    format!(
        "{}.{}.{}.eks.amazonaws.com/",
        Uuid::new_v4().simple(),
        Uuid::new_v4().simple(),
        req.region
    )
}

fn random_alnum(len: usize) -> String {
    let base = Uuid::new_v4().simple().to_string();
    base.repeat((len / base.len()) + 1)
        .chars()
        .take(len)
        .collect()
}

fn new_association_id() -> String {
    format!("a-{}", random_alnum(12).to_lowercase())
}

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "eks" {
        return None;
    }
    Some(with_store(req, |st| route(req, st)))
}

fn with_store(req: &Request, f: impl FnOnce(&mut EksStore) -> Response) -> Response {
    let slot = stores();
    let mut map = slot.write();
    let st = map.entry((req.account, req.region.clone())).or_default();
    f(st)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn ok(body: Value) -> Response {
    Response {
        status: 200,
        headers: vec![("Content-Type".to_string(), "application/json".to_string())],
        body: serde_json::to_vec(&body).unwrap_or_default(),
    }
}

/// EKS errors carry distinct statuses (404/409/400) like moto's EKSError.
fn error(status: u16, code: &str, message: &str) -> Response {
    Response {
        status,
        headers: vec![("Content-Type".to_string(), "application/json".to_string())],
        body: serde_json::to_vec(&json!({"__type": code, "message": message})).unwrap_or_default(),
    }
}

fn not_found(message: String) -> Response {
    error(404, "ResourceNotFoundException", &message)
}

fn in_use(message: String) -> Response {
    error(409, "ResourceInUseException", &message)
}

fn invalid_param(message: String) -> Response {
    error(400, "InvalidParameterException", &message)
}

fn cluster_not_found(name: &str) -> Response {
    not_found(format!("No cluster found for name: {name}."))
}

fn nodegroup_not_found(name: &str) -> Response {
    not_found(format!("No node group found for name: {name}."))
}

fn fargate_not_found(name: &str) -> Response {
    not_found(format!("No Fargate Profile found with name: {name}."))
}

fn addon_not_found(name: &str) -> Response {
    not_found(format!("No addon found for name: {name}."))
}

fn body_of(req: &Request) -> Value {
    crate::proto::parse_json_body(&req.body)
}

fn utf8(body: &Value) -> String {
    serde_json::to_string(body).unwrap_or_default()
}

fn valid_role_arn(arn: &str) -> bool {
    arn.starts_with("arn:aws:iam::") && arn.split(':').count() >= 6 && arn.contains(":role/")
}

/// botocore pagination: sorted, slice from next_token, at most max_results.
fn paginated(
    list: &[String],
    max_results: i64,
    next_token: Option<&String>,
) -> (Vec<String>, Option<String>) {
    let mut sorted: Vec<String> = list.to_vec();
    sorted.sort();
    let start = next_token
        .and_then(|t| sorted.iter().position(|e| e == t))
        .unwrap_or(0);
    let end = (start + max_results.max(0) as usize).min(sorted.len());
    let new_next = if end == sorted.len() {
        None
    } else {
        sorted.get(end).cloned()
    };
    (sorted[start..end].to_vec(), new_next)
}

/// Route an update-shaped response like moto's update objects.
fn update_object(kind: &str, params: Value) -> Value {
    json!({
        "id": Uuid::new_v4().to_string(),
        "status": "Successful",
        "type": kind,
        "params": params,
        "createdAt": now(),
        "errors": [],
    })
}

fn tags_param(params: &Value, key: &str) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    if let Some(obj) = params.get(key).and_then(Value::as_object) {
        for (k, v) in obj {
            if let Some(s) = v.as_str() {
                out.insert(k.clone(), s.to_string());
            }
        }
    }
    out
}

fn tags_json(tags: &BTreeMap<String, String>) -> Value {
    Value::Object(
        tags.iter()
            .map(|(k, v)| (k.clone(), json!(v.clone())))
            .collect(),
    )
}

// ---------------------------------------------------------------------------
// Routing
// ---------------------------------------------------------------------------

fn route(req: &Request, st: &mut EksStore) -> Response {
    let path = req.path.trim_matches('/').to_string();
    let parts: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    let method = req.method.as_str();
    let body = body_of(req);

    // Cluster collection / nested clusters
    if let Some(rest) = strip(&parts, &["clusters"]) {
        return clusters_routes(req, st, method, rest, &body);
    }
    if let Some(rest) = strip(&parts, &["tags"]) {
        return tags_routes(req, st, method, rest, &body);
    }
    if let Some(rest) = strip(&parts, &["addons"]) {
        return addon_catalog_routes(req, st, method, rest);
    }
    if let Some(rest) = strip(&parts, &["cluster-versions"]) {
        return cluster_versions_routes(req, st, method, rest, &body);
    }
    if parts == ["access-policies"] {
        return ok(json!({
            "accessPolicies": ACCESS_POLICIES.clone(),
            "nextToken": Value::Null,
        }));
    }
    if let Some(rest) = strip(&parts, &["eks-anywhere-subscriptions"]) {
        return subscriptions_routes(req, st, method, rest, &body);
    }
    if let Some(rest) = strip(&parts, &["cluster-registrations"]) {
        return registrations_routes(req, st, method, rest, &body);
    }

    not_supported(req)
}

fn strip<'a>(parts: &'a [&'a str], prefix: &[&str]) -> Option<&'a [&'a str]> {
    if parts.len() < prefix.len() {
        return None;
    }
    for (idx, p) in prefix.iter().enumerate() {
        if parts[idx] != *p {
            return None;
        }
    }
    Some(&parts[prefix.len()..])
}

fn not_supported(req: &Request) -> Response {
    crate::proto::json_error(
        501,
        "NotImplemented",
        &format!("The {} action has not been implemented", req.operation),
    )
}

// ---------------------------------------------------------------------------
// Cluster family
// ---------------------------------------------------------------------------

fn clusters_routes(
    req: &Request,
    st: &mut EksStore,
    method: &str,
    rest: &[&str],
    body: &Value,
) -> Response {
    if rest.is_empty() {
        return match method {
            "POST" => create_cluster(req, st, body),
            "GET" => list_clusters(req, st),
            _ => not_supported(req),
        };
    }
    let name = req_path_value(rest[0]);
    if !st.clusters.contains_key(&name) {
        return cluster_not_found(&name);
    }
    cluster_sub(req, st, &name, method, &rest[1..], body)
}

/// Path segments arrive URL-encoded for ARN-shaped path params.
fn req_path_value(segment: &str) -> String {
    percent_decode(segment)
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 3 <= bytes.len() => {
                match u8::from_str_radix(
                    std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or(""),
                    16,
                ) {
                    Ok(b) => {
                        out.push(b);
                        i += 3;
                    }
                    Err(_) => {
                        out.push(b'%');
                        i += 1;
                    }
                }
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Handle all subcluster paths (the cluster is known to exist).
#[allow(clippy::too_many_lines)]
fn cluster_sub(
    req: &Request,
    st: &mut EksStore,
    name: &str,
    method: &str,
    rest: &[&str],
    body: &Value,
) -> Response {
    if rest.is_empty() {
        return match method {
            "GET" => cluster_response(st, name),
            "DELETE" => delete_cluster(st, name),
            _ => not_supported(req),
        };
    }
    match (method, rest.first()) {
        ("POST", Some(&"update-config")) => update_cluster_config(st, name, body),
        ("POST", Some(&"updates")) => update_cluster_version(st, name, body),
        ("POST", Some(&"encryption-config")) => associate_encryption_config(st, name, body),
        ("POST", Some(&"node-groups")) if rest.len() == 1 => create_nodegroup(req, st, name, body),
        ("GET", Some(&"node-groups")) if rest.len() == 1 => list_nodegroups(req, st, name),
        (_, Some(&"node-groups")) if rest.len() >= 2 => {
            nodegroup_sub(req, st, name, rest[1], method, &rest[2..], body)
        }
        ("POST", Some(&"fargate-profiles")) if rest.len() == 1 => {
            create_fargate_profile(req, st, name, body)
        }
        ("GET", Some(&"fargate-profiles")) if rest.len() == 1 => {
            list_fargate_profiles(req, st, name)
        }
        (_, Some(&"fargate-profiles")) if rest.len() >= 2 => {
            fargate_sub(req, st, name, rest[1], method)
        }
        ("POST", Some(&"addons")) if rest.len() == 1 => create_addon(req, st, name, body),
        ("GET", Some(&"addons")) if rest.len() == 1 => list_addons(req, st, name),
        (_, Some(&"addons")) if rest.len() >= 2 => {
            addon_sub(req, st, name, rest[1], method, &rest[2..], body)
        }
        ("POST", Some(&"access-entries")) if rest.len() == 1 => {
            create_access_entry(req, st, name, body)
        }
        ("GET", Some(&"access-entries")) if rest.len() == 1 => list_access_entries(req, st, name),
        ("GET", Some(&"access-entries")) if rest.len() >= 2 => {
            describe_access_entry(st, name, rest[1])
        }
        ("DELETE", Some(&"access-entries")) if rest.len() >= 2 => {
            delete_access_entry(st, name, rest[1])
        }
        ("POST", Some(&"pod-identity-associations")) if rest.len() == 1 => {
            create_pod_identity_association(req, st, name, body)
        }
        ("GET", Some(&"pod-identity-associations")) if rest.len() == 1 => {
            list_pod_identity_associations(req, st, name)
        }
        (_, Some(&"pod-identity-associations")) if rest.len() >= 2 => {
            pod_association_sub(st, name, rest[1], method)
        }
        ("GET", Some(&"identity-provider-configs")) if rest.len() == 1 => {
            list_identity_provider_configs(st, name)
        }
        ("POST", Some(&"identity-provider-configs")) if rest.len() == 2 => {
            idp_config_sub(req, st, name, rest[1], body)
        }
        ("POST", Some(&"insights")) => list_insights(st, name),
        ("GET", Some(&"insights")) if rest.len() >= 2 => describe_insight(st, name, rest[1]),
        ("POST", Some(&"insights-refresh")) => start_insights_refresh(st, name),
        ("GET", Some(&"insights-refresh")) => describe_insights_refresh(st, name),
        ("POST", Some(&"capabilities")) if rest.len() == 1 => {
            create_capability(req, st, name, body)
        }
        ("GET", Some(&"capabilities")) if rest.len() == 1 => list_capabilities(st, name),
        (_, Some(&"capabilities")) if rest.len() >= 2 => {
            capability_sub(req, st, name, rest[1], method, body)
        }
        _ => not_supported(req),
    }
}

fn create_cluster(req: &Request, st: &mut EksStore, body: &Value) -> Response {
    let name = body
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    if st.clusters.contains_key(&name) {
        return in_use(format!("Cluster already exists with name: {name}"));
    }
    let role_arn = body
        .get("roleArn")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    if !valid_role_arn(&role_arn) {
        return invalid_param(format!("Invalid Role Arn: '{role_arn}'"));
    }
    let tags = tags_param(body, "tags");
    let cluster = json!({
        "name": name.clone(),
        "arn": format!("{}:cluster/{}", arn_prefix(req), name),
        "createdAt": now(),
        "certificateAuthority": {"data": random_alnum(1400)},
        "identity": {"oidc": {"issuer": issuer_for(req)}},
        "endpoint": endpoint_for(req),
        "kubernetesNetworkConfig": body
            .get("kubernetesNetworkConfig")
            .cloned()
            .unwrap_or(DEFAULT_KUBERNETES_NETWORK_CONFIG.clone()),
        "logging": body.get("logging").cloned().unwrap_or(DEFAULT_LOGGING.clone()),
        "platformVersion": DEFAULT_PLATFORM_VERSION,
        "status": ACTIVE_STATUS,
        "version": body
            .get("version")
            .and_then(Value::as_str)
            .unwrap_or(DEFAULT_KUBERNETES_VERSION),
        "encryptionConfig": body.get("encryptionConfig").cloned().unwrap_or(json!([])),
        "resourcesVpcConfig": body.get("resourcesVpcConfig").cloned().unwrap_or(json!({})),
        "roleArn": role_arn,
        "tags": tags_json(&tags),
        "remoteNetworkConfig": body.get("remoteNetworkConfig").cloned().unwrap_or(Value::Null),
    });
    st.clusters.insert(
        name.clone(),
        ClusterRecord {
            cluster: cluster.clone(),
            nodegroups: BTreeMap::new(),
            fargate_profiles: BTreeMap::new(),
            addons: BTreeMap::new(),
            access_entries: BTreeMap::new(),
            pod_identity_associations: BTreeMap::new(),
            identity_provider_configs: BTreeMap::new(),
            capabilities: BTreeMap::new(),
        },
    );
    ok(json!({ "cluster": cluster }))
}

fn list_clusters(req: &Request, st: &mut EksStore) -> Response {
    let max_results = max_results_param(req, 100);
    let next_token = query_param(req, "nextToken");
    let names: Vec<String> = st.clusters.keys().cloned().collect();
    let (list, next) = paginated(&names, max_results, next_token.as_ref());
    ok(json!({
        "clusters": list,
        "nextToken": next.map(|t| json!(t)).unwrap_or(Value::Null),
    }))
}

fn cluster_response(st: &mut EksStore, name: &str) -> Response {
    let cluster = st.clusters.get(name).map(|c| c.cluster.clone());
    match cluster {
        Some(c) => ok(json!({ "cluster": c })),
        None => cluster_not_found(name),
    }
}

fn delete_cluster(st: &mut EksStore, name: &str) -> Response {
    let Some(record) = st.clusters.get(name) else {
        return cluster_not_found(name);
    };
    if !record.nodegroups.is_empty() {
        return in_use(CLUSTER_IN_USE_MSG.to_string());
    }
    let record = st.clusters.remove(name).unwrap();
    st.insights_refreshes.remove(name);
    ok(json!({ "cluster": record.cluster }))
}

fn max_results_param(req: &Request, default: i64) -> i64 {
    query_param(req, "maxResults")
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn query_param(req: &Request, key: &str) -> Option<String> {
    req.query_string.split('&').find_map(|kv| {
        let (k, v) = kv.split_once('=').unwrap_or((kv, ""));
        (k == key).then(|| v.to_string())
    })
}

fn update_cluster_config(st: &mut EksStore, name: &str, body: &Value) -> Response {
    let Some(record) = st.clusters.get_mut(name) else {
        return cluster_not_found(name);
    };
    let cluster = &mut record.cluster;
    if let Some(v) = body.get("resourcesVpcConfig").filter(|v| !v.is_null()) {
        cluster["resourcesVpcConfig"] = v.clone();
    }
    if let Some(v) = body.get("logging").filter(|v| !v.is_null()) {
        cluster["logging"] = v.clone();
    }
    if let Some(v) = body.get("kubernetesNetworkConfig").filter(|v| !v.is_null()) {
        cluster["kubernetesNetworkConfig"] = v.clone();
    }
    if let Some(v) = body.get("remoteNetworkConfig").filter(|v| !v.is_null()) {
        cluster["remoteNetworkConfig"] = v.clone();
    }
    if let Some(v) = body.get("clientRequestToken").filter(|v| !v.is_null()) {
        cluster["clientRequestToken"] = v.clone();
    }
    ok(json!({ "cluster": cluster.clone() }))
}

fn update_cluster_version(st: &mut EksStore, name: &str, body: &Value) -> Response {
    let version = body
        .get("version")
        .and_then(Value::as_str)
        .unwrap_or(DEFAULT_KUBERNETES_VERSION)
        .to_string();
    let Some(record) = st.clusters.get_mut(name) else {
        return cluster_not_found(name);
    };
    let platform = record.cluster["platformVersion"].clone();
    record.cluster["version"] = json!(version.clone());
    ok(json!({ "update": update_object(
        "VersionUpdate",
        json!([
            {"type": "Version", "value": version},
            {"type": "PlatformVersion", "value": platform},
        ])
    ) }))
}

fn associate_encryption_config(st: &mut EksStore, name: &str, body: &Value) -> Response {
    let Some(record) = st.clusters.get_mut(name) else {
        return cluster_not_found(name);
    };
    let config = body.get("encryptionConfig").cloned().unwrap_or(json!([]));
    let cluster = &mut record.cluster;
    if let Some(existing) = cluster["encryptionConfig"].as_array_mut() {
        if let Some(extra) = config.as_array() {
            for item in extra {
                existing.push(item.clone());
            }
        }
    }
    ok(json!({ "update": update_object(
        "AssociateEncryptionConfig",
        json!([{"type": "EncryptionConfig", "value": utf8(&config)}])
    ) }))
}

// ---------------------------------------------------------------------------
// Nodegroups
// ---------------------------------------------------------------------------

fn create_nodegroup(
    req: &Request,
    st: &mut EksStore,
    cluster_name: &str,
    body: &Value,
) -> Response {
    let nodegroup_name = body
        .get("nodegroupName")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let Some(record) = st.clusters.get_mut(cluster_name) else {
        return cluster_not_found(cluster_name);
    };
    if record.nodegroups.contains_key(&nodegroup_name) {
        return in_use(format!(
            "NodeGroup already exists with name {nodegroup_name} and cluster name {cluster_name}"
        ));
    }
    let node_role = body
        .get("nodeRole")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    if !valid_role_arn(&node_role) {
        return invalid_param(format!("Invalid Role Arn: '{node_role}'"));
    }
    let uuid = Uuid::new_v4().to_string();
    let nodegroup = json!({
        "nodegroupName": nodegroup_name.clone(),
        "clusterName": cluster_name,
        "nodegroupArn": format!(
            "{}:nodegroup/{cluster_name}/{nodegroup_name}/{uuid}",
            arn_prefix(req)
        ),
        "createdAt": now(),
        "modifiedAt": now(),
        "health": {"issues": []},
        "resources": {
            "autoScalingGroups": [{"name": format!("eks-{uuid}")}],
            "remoteAccessSecurityGroup": format!("sg-{}", random_alnum(17).to_lowercase()),
        },
        "amiType": body.get("amiType").and_then(Value::as_str).unwrap_or(DEFAULT_AMI_TYPE),
        "capacityType": body
            .get("capacityType")
            .and_then(Value::as_str)
            .unwrap_or(DEFAULT_CAPACITY_TYPE),
        "diskSize": body.get("diskSize").and_then(Value::as_i64).unwrap_or(20),
        "instanceTypes": body.get("instanceTypes").cloned().unwrap_or(json!(["t3.medium"])),
        "releaseVersion": body
            .get("releaseVersion")
            .and_then(Value::as_str)
            .unwrap_or(DEFAULT_RELEASE_VERSION),
        "remoteAccess": body
            .get("remoteAccess")
            .cloned()
            .unwrap_or(DEFAULT_REMOTE_ACCESS.clone()),
        "scalingConfig": body
            .get("scalingConfig")
            .cloned()
            .unwrap_or(DEFAULT_SCALING_CONFIG.clone()),
        "status": ACTIVE_STATUS,
        "version": body
            .get("version")
            .and_then(Value::as_str)
            .unwrap_or(DEFAULT_KUBERNETES_VERSION),
        "clientRequestToken": body.get("clientRequestToken").cloned().unwrap_or(Value::Null),
        "labels": body.get("labels").cloned().unwrap_or(json!({})),
        "launchTemplate": body.get("launchTemplate").cloned().unwrap_or(Value::Null),
        "nodeRole": node_role,
        "subnets": body.get("subnets").cloned().unwrap_or(json!([])),
        "tags": tags_json(&tags_param(body, "tags")),
        "taints": body.get("taints").cloned().unwrap_or(json!([])),
        "updateConfig": body.get("updateConfig").cloned().unwrap_or(Value::Null),
        "nodeRepairConfig": body.get("nodeRepairConfig").cloned().unwrap_or(Value::Null),
    });
    record.nodegroups.insert(nodegroup_name, nodegroup.clone());
    ok(json!({ "nodegroup": nodegroup }))
}

fn list_nodegroups(req: &Request, st: &mut EksStore, cluster_name: &str) -> Response {
    let Some(record) = st.clusters.get(cluster_name) else {
        return cluster_not_found(cluster_name);
    };
    let names: Vec<String> = record.nodegroups.keys().cloned().collect();
    let max_results = max_results_param(req, 100);
    let next_token = query_param(req, "nextToken");
    let (list, next) = paginated(&names, max_results, next_token.as_ref());
    ok(json!({
        "nodegroups": list,
        "nextToken": next.map(|t| json!(t)).unwrap_or(Value::Null),
    }))
}

fn get_nodegroup(st: &mut EksStore, cluster_name: &str, nodegroup_name: &str) -> Option<Value> {
    st.clusters
        .get(cluster_name)
        .and_then(|c| c.nodegroups.get(nodegroup_name))
        .cloned()
}

fn nodegroup_sub(
    req: &Request,
    st: &mut EksStore,
    cluster_name: &str,
    nodegroup_name: &str,
    method: &str,
    rest: &[&str],
    body: &Value,
) -> Response {
    // Cluster existence is checked with moto's distinct error first.
    if !st.clusters.contains_key(cluster_name) {
        return cluster_not_found(cluster_name);
    }
    let ng_name = req_path_value(nodegroup_name);
    match (method, rest.first()) {
        ("GET", None) => match get_nodegroup(st, cluster_name, &ng_name) {
            Some(ng) => ok(json!({ "nodegroup": ng })),
            None => nodegroup_not_found(&ng_name),
        },
        ("DELETE", None) => {
            let ng = st
                .clusters
                .get_mut(cluster_name)
                .and_then(|c| c.nodegroups.remove(&ng_name));
            match ng {
                Some(ng) => ok(json!({ "nodegroup": ng })),
                None => nodegroup_not_found(&ng_name),
            }
        }
        ("POST", Some(&"update-config")) => {
            update_nodegroup_config(st, cluster_name, &ng_name, body)
        }
        _ => not_supported(req),
    }
}

fn update_nodegroup_config(
    st: &mut EksStore,
    cluster_name: &str,
    nodegroup_name: &str,
    body: &Value,
) -> Response {
    let Some(record) = st.clusters.get_mut(cluster_name) else {
        return cluster_not_found(cluster_name);
    };
    let Some(ng) = record.nodegroups.get_mut(nodegroup_name) else {
        return nodegroup_not_found(nodegroup_name);
    };
    let mut params: Vec<Value> = Vec::new();
    if let Some(labels) = body.get("labels") {
        if let Some(add) = labels.get("addOrUpdateLabels").and_then(Value::as_object) {
            let stored = ng["labels"].as_object_mut().unwrap();
            for (k, v) in add {
                stored.insert(k.clone(), v.clone());
            }
        }
        if let Some(remove) = labels.get("removeLabels").and_then(Value::as_array) {
            let stored = ng["labels"].as_object_mut().unwrap();
            for key in remove.iter().filter_map(Value::as_str) {
                stored.remove(key);
            }
        }
        params.push(json!({"type": "Labels", "value": utf8(labels)}));
    }
    if let Some(taints) = body.get("taints") {
        let stored = ng["taints"].as_array_mut().unwrap();
        let mutate = |stored: &mut Vec<Value>, new_taint: &Value, add: bool| {
            if let Some(obj) = new_taint.as_object() {
                let key = obj.get("key").and_then(Value::as_str).unwrap_or("");
                let effect = obj.get("effect").and_then(Value::as_str).unwrap_or("");
                stored.retain(|t| {
                    !(t.get("key").and_then(Value::as_str).unwrap_or("") == key
                        && t.get("effect").and_then(Value::as_str).unwrap_or("") == effect)
                });
                if add {
                    stored.push(new_taint.clone());
                }
            }
        };
        if let Some(add) = taints.get("addOrUpdateTaints").and_then(Value::as_array) {
            for tain in add {
                mutate(stored, tain, true);
            }
        }
        if let Some(remove) = taints.get("removeTaints").and_then(Value::as_array) {
            for tain in remove {
                mutate(stored, tain, false);
            }
        }
        params.push(json!({"type": "Taints", "value": utf8(taints)}));
    }
    if let Some(scaling) = body.get("scalingConfig").and_then(Value::as_object) {
        let stored = ng["scalingConfig"].as_object_mut().unwrap();
        for (k, v) in scaling {
            stored.insert(k.clone(), v.clone());
        }
        params.push(json!({
            "type": "ScalingConfig",
            "value": utf8(&Value::Object(scaling.clone())),
        }));
    }
    if let Some(config) = body.get("updateConfig").filter(|v| !v.is_null()) {
        ng["updateConfig"] = config.clone();
        params.push(json!({"type": "UpdateConfig", "value": utf8(config)}));
    }
    if let Some(config) = body.get("nodeRepairConfig").filter(|v| !v.is_null()) {
        ng["nodeRepairConfig"] = config.clone();
        params.push(json!({"type": "NodeRepairConfig", "value": utf8(config)}));
    }
    ng["modifiedAt"] = json!(now());
    ok(json!({ "update": update_object("ConfigUpdate", json!(params)) }))
}

// ---------------------------------------------------------------------------
// Fargate profiles
// ---------------------------------------------------------------------------

fn create_fargate_profile(
    req: &Request,
    st: &mut EksStore,
    cluster_name: &str,
    body: &Value,
) -> Response {
    let profile_name = body
        .get("fargateProfileName")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let selectors = body.get("selectors").cloned().unwrap_or(json!([]));
    let non_empty = selectors.as_array().is_some_and(|s| !s.is_empty());
    if !non_empty {
        return invalid_param("Fargate Profile requires at least one selector.".to_string());
    }
    let all_missing_ns = selectors.as_array().is_some_and(|sels| {
        sels.iter().all(|s| {
            s.get("namespace")
                .is_none_or(|v| v.as_str().unwrap_or("").is_empty())
        })
    });
    if all_missing_ns {
        return invalid_param(
            "Fargate Profile must have at least one selector with at least one namespace value."
                .to_string(),
        );
    }
    let Some(record) = st.clusters.get_mut(cluster_name) else {
        return cluster_not_found(cluster_name);
    };
    if record.fargate_profiles.contains_key(&profile_name) {
        return in_use(
            "A Fargate Profile already exists with this name in this cluster.".to_string(),
        );
    }
    let uuid = Uuid::new_v4().to_string();
    let mut profile = json!({
        "clusterName": cluster_name,
        "fargateProfileName": profile_name.clone(),
        "fargateProfileArn": format!(
            "{}:fargateprofile/{cluster_name}/{profile_name}/{uuid}",
            arn_prefix(req)
        ),
        "createdAt": now(),
        "status": ACTIVE_STATUS,
        "podExecutionRoleArn": body
            .get("podExecutionRoleArn")
            .and_then(Value::as_str)
            .unwrap_or(""),
        "clientRequestToken": body.get("clientRequestToken").cloned().unwrap_or(Value::Null),
        "selectors": selectors,
        "subnets": body.get("subnets").cloned().unwrap_or(json!([])),
        "tags": tags_json(&tags_param(body, "tags")),
    });
    profile.as_object_mut().unwrap().retain(|_, v| !v.is_null());
    record
        .fargate_profiles
        .insert(profile_name, profile.clone());
    ok(json!({ "fargateProfile": profile }))
}

fn list_fargate_profiles(req: &Request, st: &mut EksStore, cluster_name: &str) -> Response {
    let Some(record) = st.clusters.get(cluster_name) else {
        return cluster_not_found(cluster_name);
    };
    let names: Vec<String> = record.fargate_profiles.keys().cloned().collect();
    let (list, next) = paginated(
        &names,
        max_results_param(req, 100),
        query_param(req, "nextToken").as_ref(),
    );
    ok(json!({
        "fargateProfileNames": list,
        "nextToken": next.map(|t| json!(t)).unwrap_or(Value::Null),
    }))
}

fn fargate_sub(
    req: &Request,
    st: &mut EksStore,
    cluster_name: &str,
    profile_name: &str,
    method: &str,
) -> Response {
    if !st.clusters.contains_key(cluster_name) {
        return cluster_not_found(cluster_name);
    }
    let profile_name = req_path_value(profile_name);
    match method {
        "GET" => match st
            .clusters
            .get(cluster_name)
            .and_then(|c| c.fargate_profiles.get(&profile_name))
        {
            Some(p) => ok(json!({ "fargateProfile": p.clone() })),
            None => fargate_not_found(&profile_name),
        },
        "DELETE" => {
            let profile = st
                .clusters
                .get_mut(cluster_name)
                .and_then(|c| c.fargate_profiles.remove(&profile_name));
            match profile {
                Some(p) => ok(json!({ "fargateProfile": p })),
                None => fargate_not_found(&profile_name),
            }
        }
        _ => not_supported(req),
    }
}

// ---------------------------------------------------------------------------
// Addons
// ---------------------------------------------------------------------------

fn create_addon(req: &Request, st: &mut EksStore, cluster_name: &str, body: &Value) -> Response {
    let addon_name = body
        .get("addonName")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let Some(record) = st.clusters.get_mut(cluster_name) else {
        return cluster_not_found(cluster_name);
    };
    if record.addons.contains_key(&addon_name) {
        return in_use(format!(
            "Addon already exists with name {addon_name} and cluster name {cluster_name}"
        ));
    }
    let uuid = Uuid::new_v4().to_string();
    let tags = tags_param(body, "tags");
    let addon = json!({
        "addonName": addon_name.clone(),
        "clusterName": cluster_name,
        "addonArn": format!("{}:addon/{cluster_name}/{addon_name}/{uuid}", arn_prefix(req)),
        "createdAt": now(),
        "modifiedAt": now(),
        "status": ACTIVE_STATUS,
        "health": {"issues": []},
        "addonVersion": body
            .get("addonVersion")
            .and_then(Value::as_str)
            .unwrap_or("v1.0.0-eksbuild.1"),
        "serviceAccountRoleArn": body
            .get("serviceAccountRoleArn")
            .and_then(Value::as_str)
            .unwrap_or(""),
        "resolveConflicts": body.get("resolveConflicts").and_then(Value::as_str).unwrap_or(""),
        "clientRequestToken": body.get("clientRequestToken").and_then(Value::as_str).unwrap_or(""),
        "tags": tags_json(&tags),
        "configurationValues": body
            .get("configurationValues")
            .and_then(Value::as_str)
            .unwrap_or(""),
    });
    record.addons.insert(addon_name, addon.clone());
    ok(json!({ "addon": addon }))
}

fn list_addons(req: &Request, st: &mut EksStore, cluster_name: &str) -> Response {
    let Some(record) = st.clusters.get(cluster_name) else {
        return cluster_not_found(cluster_name);
    };
    let names: Vec<String> = record.addons.keys().cloned().collect();
    let (list, next) = paginated(
        &names,
        max_results_param(req, 100),
        query_param(req, "nextToken").as_ref(),
    );
    ok(json!({
        "addons": list,
        "nextToken": next.map(|t| json!(t)).unwrap_or(Value::Null),
    }))
}

fn addon_sub(
    req: &Request,
    st: &mut EksStore,
    cluster_name: &str,
    addon_name: &str,
    method: &str,
    rest: &[&str],
    body: &Value,
) -> Response {
    if !st.clusters.contains_key(cluster_name) {
        return cluster_not_found(cluster_name);
    }
    let addon_name = req_path_value(addon_name);
    match (method, rest.first()) {
        ("GET", None) => match get_addon(st, cluster_name, &addon_name) {
            Some(addon) => ok(json!({ "addon": addon })),
            None => addon_not_found(&addon_name),
        },
        ("DELETE", None) => {
            let addon = st
                .clusters
                .get_mut(cluster_name)
                .and_then(|c| c.addons.remove(&addon_name));
            match addon {
                Some(mut addon) => {
                    addon["status"] = json!("DELETING");
                    ok(json!({ "addon": addon }))
                }
                None => addon_not_found(&addon_name),
            }
        }
        ("POST", Some(&"update")) => update_addon(st, cluster_name, &addon_name, body),
        _ => not_supported(req),
    }
}

fn get_addon(st: &mut EksStore, cluster_name: &str, addon_name: &str) -> Option<Value> {
    st.clusters
        .get(cluster_name)
        .and_then(|c| c.addons.get(addon_name))
        .cloned()
}

fn update_addon(st: &mut EksStore, cluster_name: &str, addon_name: &str, body: &Value) -> Response {
    let Some(record) = st.clusters.get_mut(cluster_name) else {
        return cluster_not_found(cluster_name);
    };
    let Some(addon) = record.addons.get_mut(addon_name) else {
        return addon_not_found(addon_name);
    };
    let mut params: Vec<Value> = Vec::new();
    if let Some(v) = body
        .get("addonVersion")
        .and_then(Value::as_str)
        .filter(|v| !v.is_empty())
    {
        addon["addonVersion"] = json!(v);
        params.push(json!({"type": "AddonVersion", "value": v}));
    }
    if let Some(v) = body
        .get("serviceAccountRoleArn")
        .and_then(Value::as_str)
        .filter(|v| !v.is_empty())
    {
        addon["serviceAccountRoleArn"] = json!(v);
        params.push(json!({"type": "ServiceAccountRoleArn", "value": v}));
    }
    if let Some(v) = body
        .get("configurationValues")
        .and_then(Value::as_str)
        .filter(|v| !v.is_empty())
    {
        addon["configurationValues"] = json!(v);
        params.push(json!({"type": "ConfigurationValues", "value": v}));
    }
    if let Some(v) = body.get("resolveConflicts").and_then(Value::as_str) {
        addon["resolveConflicts"] = json!(v);
    }
    addon["modifiedAt"] = json!(now());
    ok(json!({ "update": update_object("AddonUpdate", json!(params)) }))
}

// ---------------------------------------------------------------------------
// Addon catalog (DescribeAddonVersions / DescribeAddonConfiguration)
// ---------------------------------------------------------------------------

fn addon_catalog_routes(req: &Request, st: &mut EksStore, method: &str, rest: &[&str]) -> Response {
    let _ = (st, method);
    if rest == ["configuration-schemas"] {
        let addon_name = query_param(req, "addonName").unwrap_or_default();
        let addon_version = query_param(req, "addonVersion").unwrap_or_default();
        return ok(json!({
            "addonName": addon_name,
            "addonVersion": addon_version,
            "configurationSchema": "{}",
            "podIdentityConfiguration": [],
        }));
    }
    if rest == ["supported-versions"] {
        return ok(json!({"addons": ADDON_VERSIONS.clone(), "nextToken": Value::Null}));
    }
    not_supported(req)
}

// ---------------------------------------------------------------------------
// Access entries
// ---------------------------------------------------------------------------

fn access_entry_public(req: &Request, cluster_name: &str, body: &Value, principal: &str) -> Value {
    let suffix = principal.rsplit('/').next().unwrap_or(principal);
    let mut entry = json!({
        "clusterName": cluster_name,
        "principalArn": principal,
        "accessEntryArn": format!(
            "{}:access-entry/{cluster_name}/{suffix}/{}",
            arn_prefix(req),
            Uuid::new_v4()
        ),
        "createdAt": now(),
        "modifiedAt": now(),
        "kubernetesGroups": body.get("kubernetesGroups").cloned().unwrap_or(json!([])),
        "username": body.get("username").and_then(Value::as_str).unwrap_or(""),
        "type": body.get("type").and_then(Value::as_str).unwrap_or("STANDARD"),
    });
    entry["tags"] = body
        .get("tags")
        .and_then(Value::as_object)
        .map(|t| Value::Object(t.clone()))
        .unwrap_or(json!({}));
    entry
}

fn create_access_entry(
    req: &Request,
    st: &mut EksStore,
    cluster_name: &str,
    body: &Value,
) -> Response {
    let principal = body
        .get("principalArn")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let Some(record) = st.clusters.get_mut(cluster_name) else {
        return cluster_not_found(cluster_name);
    };
    if record.access_entries.contains_key(&principal) {
        return in_use(format!(
            "Access entry already exists for principal {principal} in cluster {cluster_name}"
        ));
    }
    let access_entry = access_entry_public(req, cluster_name, body, &principal);
    record
        .access_entries
        .insert(principal, access_entry.clone());
    ok(json!({ "accessEntry": access_entry }))
}

fn describe_access_entry(st: &mut EksStore, cluster_name: &str, principal: &str) -> Response {
    if !st.clusters.contains_key(cluster_name) {
        return cluster_not_found(cluster_name);
    }
    let principal = req_path_value(principal);
    match st
        .clusters
        .get(cluster_name)
        .and_then(|c| c.access_entries.get(&principal))
    {
        Some(entry) => ok(json!({ "accessEntry": entry.clone() })),
        None => not_found(format!("No access entry found for principal: {principal}.")),
    }
}

fn delete_access_entry(st: &mut EksStore, cluster_name: &str, principal: &str) -> Response {
    if !st.clusters.contains_key(cluster_name) {
        return cluster_not_found(cluster_name);
    }
    let principal = req_path_value(principal);
    match st
        .clusters
        .get_mut(cluster_name)
        .and_then(|c| c.access_entries.remove(&principal))
    {
        Some(_) => ok(json!({})),
        None => not_found(format!("No access entry found for principal: {principal}.")),
    }
}

fn list_access_entries(req: &Request, st: &mut EksStore, cluster_name: &str) -> Response {
    let Some(record) = st.clusters.get(cluster_name) else {
        return cluster_not_found(cluster_name);
    };
    let names: Vec<String> = record.access_entries.keys().cloned().collect();
    let (list, next) = paginated(
        &names,
        max_results_param(req, 100),
        query_param(req, "nextToken").as_ref(),
    );
    ok(json!({
        "accessEntries": list,
        "nextToken": next.map(|t| json!(t)).unwrap_or(Value::Null),
    }))
}

// ---------------------------------------------------------------------------
// Pod identity associations
// ---------------------------------------------------------------------------

fn pod_association_public(req: &Request, cluster_name: &str, body: &Value) -> Value {
    let uuid = Uuid::new_v4().to_string();
    json!({
        "clusterName": cluster_name,
        "namespace": body.get("namespace").and_then(Value::as_str).unwrap_or(""),
        "serviceAccount": body.get("serviceAccount").and_then(Value::as_str).unwrap_or(""),
        "roleArn": body.get("roleArn").and_then(Value::as_str).unwrap_or(""),
        "associationId": new_association_id(),
        "associationArn": format!(
            "{}:podidentityassociation/{cluster_name}/{uuid}",
            arn_prefix(req)
        ),
        "createdAt": now(),
        "modifiedAt": now(),
        "ownerArn": Value::Null,
        "tags": body
            .get("tags")
            .and_then(Value::as_object)
            .map(|t| Value::Object(t.clone()))
            .unwrap_or(json!({})),
    })
}

fn create_pod_identity_association(
    req: &Request,
    st: &mut EksStore,
    cluster_name: &str,
    body: &Value,
) -> Response {
    let Some(record) = st.clusters.get_mut(cluster_name) else {
        return cluster_not_found(cluster_name);
    };
    let association = pod_association_public(req, cluster_name, body);
    let association_id = association["associationId"]
        .as_str()
        .unwrap_or("")
        .to_string();
    record
        .pod_identity_associations
        .insert(association_id, association.clone());
    ok(json!({ "association": association }))
}

fn list_pod_identity_associations(
    _req: &Request,
    st: &mut EksStore,
    cluster_name: &str,
) -> Response {
    let Some(record) = st.clusters.get(cluster_name) else {
        return cluster_not_found(cluster_name);
    };
    let summary: Vec<Value> = record
        .pod_identity_associations
        .values()
        .map(|a| {
            json!({
                "clusterName": a["clusterName"],
                "namespace": a["namespace"],
                "serviceAccount": a["serviceAccount"],
                "associationArn": a["associationArn"],
                "associationId": a["associationId"],
                "ownerArn": a["ownerArn"],
            })
        })
        .collect();
    ok(json!({"associations": summary, "nextToken": Value::Null}))
}

fn pod_association_sub(
    st: &mut EksStore,
    cluster_name: &str,
    association_id: &str,
    method: &str,
) -> Response {
    if !st.clusters.contains_key(cluster_name) {
        return cluster_not_found(cluster_name);
    }
    match method {
        "GET" => match st
            .clusters
            .get(cluster_name)
            .and_then(|c| c.pod_identity_associations.get(association_id))
        {
            Some(a) => ok(json!({ "association": a.clone() })),
            None => not_found(format!(
                "No pod identity association found for id: {association_id}."
            )),
        },
        "DELETE" => {
            let association = st
                .clusters
                .get_mut(cluster_name)
                .and_then(|c| c.pod_identity_associations.remove(association_id));
            match association {
                Some(a) => ok(json!({ "association": a })),
                None => not_found(format!(
                    "No pod identity association found for id: {association_id}."
                )),
            }
        }
        _ => crate::proto::json_error(501, "NotImplemented", "Unsupported pod identity op"),
    }
}

// ---------------------------------------------------------------------------
// Identity provider configs
// ---------------------------------------------------------------------------

fn idp_config_public(req: &Request, cluster_name: &str, oidc: &Value) -> Value {
    let config_name = oidc
        .get("identityProviderConfigName")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    json!({
        "clusterName": cluster_name,
        "configName": config_name.clone(),
        "issuerUrl": oidc.get("issuerUrl").and_then(Value::as_str).unwrap_or(""),
        "clientId": oidc.get("clientId").and_then(Value::as_str).unwrap_or(""),
        "groupsClaim": oidc.get("groupsClaim").cloned().unwrap_or(Value::Null),
        "groupsPrefix": oidc.get("groupsPrefix").cloned().unwrap_or(Value::Null),
        "requiredClaims": oidc.get("requiredClaims").cloned().unwrap_or(json!({})),
        "usernameClaim": oidc.get("usernameClaim").cloned().unwrap_or(Value::Null),
        "usernamePrefix": oidc.get("usernamePrefix").cloned().unwrap_or(Value::Null),
        "tags": oidc
            .get("tags")
            .and_then(Value::as_object)
            .map(|t| Value::Object(t.clone()))
            .unwrap_or(json!({})),
        "status": ACTIVE_STATUS,
        "identityProviderConfigArn": format!(
            "{}:identityproviderconfig/{cluster_name}/oidc/{config_name}/{}",
            arn_prefix(req),
            Uuid::new_v4()
        ),
        "createdAt": now(),
    })
}

fn idp_config_sub(
    req: &Request,
    st: &mut EksStore,
    cluster_name: &str,
    sub: &str,
    body: &Value,
) -> Response {
    match sub {
        "associate" => {
            let Some(record) = st.clusters.get_mut(cluster_name) else {
                return cluster_not_found(cluster_name);
            };
            let oidc = body.get("oidc").cloned().unwrap_or(json!({}));
            let config_name = oidc
                .get("identityProviderConfigName")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            if record.identity_provider_configs.contains_key(&config_name) {
                return in_use(format!(
                    "Identity provider config already exists with name {config_name} and cluster name {cluster_name}"
                ));
            }
            record
                .identity_provider_configs
                .insert(config_name, idp_config_public(req, cluster_name, &oidc));
            ok(json!({ "update": update_object(
                "AssociateIdentityProviderConfig",
                json!([{"type": "IdentityProviderConfig", "value": utf8(&oidc)}])
            ) }))
        }
        "disassociate" => {
            let Some(record) = st.clusters.get_mut(cluster_name) else {
                return cluster_not_found(cluster_name);
            };
            let config_name = body
                .get("identityProviderConfig")
                .and_then(|c| c.get("name"))
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            if record
                .identity_provider_configs
                .remove(&config_name)
                .is_none()
            {
                return not_found(format!(
                    "No identity provider config found for name: {config_name}."
                ));
            }
            ok(json!({ "update": update_object(
                "DisassociateIdentityProviderConfig",
                json!([])
            ) }))
        }
        "describe" => {
            let Some(record) = st.clusters.get(cluster_name) else {
                return cluster_not_found(cluster_name);
            };
            let config_name = body
                .get("identityProviderConfig")
                .and_then(|c| c.get("name"))
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            match record.identity_provider_configs.get(&config_name) {
                Some(config) => ok(json!({"identityProviderConfig": {"oidc": {
                    "identityProviderConfigName": config["configName"].clone(),
                    "identityProviderConfigArn": config["identityProviderConfigArn"].clone(),
                    "clusterName": config["clusterName"].clone(),
                    "issuerUrl": config["issuerUrl"].clone(),
                    "clientId": config["clientId"].clone(),
                    "usernameClaim": config["usernameClaim"].clone(),
                    "usernamePrefix": config["usernamePrefix"].clone(),
                    "groupsClaim": config["groupsClaim"].clone(),
                    "groupsPrefix": config["groupsPrefix"].clone(),
                    "requiredClaims": config["requiredClaims"].clone(),
                    "tags": config["tags"].clone(),
                    "status": {"status": config["status"].clone()},
                }}})),
                None => not_found(format!(
                    "No identity provider config found for name: {config_name}."
                )),
            }
        }
        _ => not_supported_pod(),
    }
}

fn not_supported_pod() -> Response {
    crate::proto::json_error(501, "NotImplemented", "Unsupported pod identity op")
}

fn list_identity_provider_configs(st: &mut EksStore, cluster_name: &str) -> Response {
    let Some(record) = st.clusters.get(cluster_name) else {
        return cluster_not_found(cluster_name);
    };
    let configs: Vec<Value> = record
        .identity_provider_configs
        .values()
        .map(|c| json!({"type": "oidc", "name": c["configName"]}))
        .collect();
    ok(json!({"identityProviderConfigs": configs, "nextToken": Value::Null}))
}

// ---------------------------------------------------------------------------
// Insights
// ---------------------------------------------------------------------------

fn describe_insight(st: &mut EksStore, cluster_name: &str, insight_id: &str) -> Response {
    let _ = st.clusters.get(cluster_name);
    not_found(format!("No insight found for id: {insight_id}."))
}

fn list_insights(st: &mut EksStore, cluster_name: &str) -> Response {
    if !st.clusters.contains_key(cluster_name) {
        return cluster_not_found(cluster_name);
    }
    ok(json!({"insights": [], "nextToken": Value::Null}))
}

fn start_insights_refresh(st: &mut EksStore, cluster_name: &str) -> Response {
    let Some(record) = st.clusters.get_mut(cluster_name) else {
        return cluster_not_found(cluster_name);
    };
    let _ = record;
    let refreshes = st
        .insights_refreshes
        .entry(cluster_name.to_string())
        .or_default();
    refreshes.insert(
        0,
        json!({
            "status": "COMPLETED",
            "message": "Insights refresh completed successfully.",
            "startedAt": now(),
            "endedAt": now(),
        }),
    );
    ok(json!({
        "status": "COMPLETED",
        "message": "Insights refresh completed successfully.",
    }))
}

fn describe_insights_refresh(st: &mut EksStore, cluster_name: &str) -> Response {
    if !st.clusters.contains_key(cluster_name) {
        return cluster_not_found(cluster_name);
    }
    match st
        .insights_refreshes
        .get(cluster_name)
        .and_then(|v| v.first().cloned())
    {
        Some(record) => ok(json!({
            "status": record["status"].clone(),
            "message": record["message"].clone(),
            "startedAt": record["startedAt"].clone(),
            "endedAt": record["endedAt"].clone(),
        })),
        None => not_found(format!(
            "No insights refresh operation found for cluster: {cluster_name}."
        )),
    }
}

// ---------------------------------------------------------------------------
// Capabilities
// ---------------------------------------------------------------------------

fn create_capability(
    req: &Request,
    st: &mut EksStore,
    cluster_name: &str,
    body: &Value,
) -> Response {
    let capability_name = body
        .get("capabilityName")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let Some(record) = st.clusters.get_mut(cluster_name) else {
        return cluster_not_found(cluster_name);
    };
    if record.capabilities.contains_key(&capability_name) {
        return in_use(format!(
            "Capability already exists with name {capability_name} and cluster name {cluster_name}"
        ));
    }
    let uuid = Uuid::new_v4().to_string();
    let mut capability = json!({
        "clusterName": cluster_name,
        "capabilityName": capability_name.clone(),
        "arn": format!(
            "{}:capability/{cluster_name}/{capability_name}/{uuid}",
            arn_prefix(req)
        ),
        "createdAt": now(),
        "modifiedAt": now(),
        "type": body.get("type").and_then(Value::as_str).unwrap_or("STANDARD"),
        "roleArn": body.get("roleArn").and_then(Value::as_str).unwrap_or(""),
        "configuration": body.get("configuration").cloned().unwrap_or(json!({})),
        "status": ACTIVE_STATUS,
        "deletePropagationPolicy": body
            .get("deletePropagationPolicy")
            .and_then(Value::as_str)
            .unwrap_or("DELETE"),
    });
    if let Some(t) = body.get("tags").and_then(Value::as_object) {
        capability["tags"] = Value::Object(t.clone());
    }
    record
        .capabilities
        .insert(capability_name, capability.clone());
    ok(json!({ "capability": capability }))
}

fn list_capabilities(st: &mut EksStore, cluster_name: &str) -> Response {
    let Some(record) = st.clusters.get(cluster_name) else {
        return cluster_not_found(cluster_name);
    };
    let capabilities: Vec<Value> = record
        .capabilities
        .values()
        .map(|c| {
            json!({
                "capabilityName": c["capabilityName"],
                "arn": c["arn"],
                "status": c["status"],
                "type": c["type"],
            })
        })
        .collect();
    ok(json!({"capabilities": capabilities, "nextToken": Value::Null}))
}

fn capability_sub(
    _req: &Request,
    st: &mut EksStore,
    cluster_name: &str,
    capability_name: &str,
    method: &str,
    body: &Value,
) -> Response {
    if !st.clusters.contains_key(cluster_name) {
        return cluster_not_found(cluster_name);
    }
    match method {
        "GET" => match st
            .clusters
            .get(cluster_name)
            .and_then(|c| c.capabilities.get(capability_name))
        {
            Some(cap) => ok(json!({ "capability": cap.clone() })),
            None => not_found(format!("No capability found for name: {capability_name}.")),
        },
        "DELETE" => {
            let capability = st
                .clusters
                .get_mut(cluster_name)
                .and_then(|c| c.capabilities.remove(capability_name));
            match capability {
                Some(mut cap) => {
                    cap["status"] = json!("DELETING");
                    ok(json!({ "capability": cap }))
                }
                None => not_found(format!("No capability found for name: {capability_name}.")),
            }
        }
        "POST" => {
            let Some(record) = st.clusters.get_mut(cluster_name) else {
                return cluster_not_found(cluster_name);
            };
            let Some(cap) = record.capabilities.get_mut(capability_name) else {
                return not_found(format!("No capability found for name: {capability_name}."));
            };
            let mut params: Vec<Value> = Vec::new();
            if let Some(v) = body
                .get("roleArn")
                .and_then(Value::as_str)
                .filter(|v| !v.is_empty())
            {
                cap["roleArn"] = json!(v);
                params.push(json!({"type": "RoleArn", "value": v}));
            }
            if let Some(v) = body.get("configuration").filter(|v| !v.is_null()) {
                cap["configuration"] = v.clone();
                params.push(json!({"type": "Configuration", "value": utf8(v)}));
            }
            if let Some(v) = body.get("deletePropagationPolicy").and_then(Value::as_str) {
                cap["deletePropagationPolicy"] = json!(v);
            }
            cap["modifiedAt"] = json!(now());
            ok(json!({ "update": update_object("CapabilityUpdate", json!(params)) }))
        }
        _ => not_supported_pod(),
    }
}

// ---------------------------------------------------------------------------
// EKS Anywhere subscriptions
// ---------------------------------------------------------------------------

fn subscription_public(req: &Request, body: &Value) -> Value {
    let uuid = Uuid::new_v4().to_string();
    let created = now();
    json!({
        "id": uuid.clone(),
        "arn": format!("{}:eks-anywhere-subscription/{uuid}", arn_prefix(req)),
        "createdAt": created,
        "effectiveDate": created,
        "expirationDate": Value::Null,
        "licenseQuantity": body.get("licenseQuantity").and_then(Value::as_i64).unwrap_or(1),
        "licenseType": body.get("licenseType").and_then(Value::as_str).unwrap_or("Cluster"),
        "name": body
            .get("name")
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| format!("subscription-{}", &uuid[..8])),
        "term": body
            .get("term")
            .cloned()
            .unwrap_or(json!({"duration": 12, "unit": "MONTHS"})),
        "status": ACTIVE_STATUS,
        "autoRenew": body.get("autoRenew").and_then(Value::as_bool).unwrap_or(true),
        "tags": body
            .get("tags")
            .and_then(Value::as_object)
            .map(|t| Value::Object(t.clone()))
            .unwrap_or(json!({})),
    })
}

fn subscriptions_routes(
    req: &Request,
    st: &mut EksStore,
    method: &str,
    rest: &[&str],
    body: &Value,
) -> Response {
    match (method, rest) {
        ("POST", []) => {
            let subscription = subscription_public(req, body);
            let id = subscription["id"].as_str().unwrap_or("").to_string();
            st.eks_anywhere_subscriptions
                .insert(id, subscription.clone());
            ok(json!({ "subscription": subscription }))
        }
        ("GET", []) => {
            let list: Vec<Value> = st.eks_anywhere_subscriptions.values().cloned().collect();
            ok(json!({"subscriptions": list, "nextToken": Value::Null}))
        }
        ("GET", [id]) => {
            let id = req_path_value(id);
            match st.eks_anywhere_subscriptions.get(&id) {
                Some(subscription) => ok(json!({ "subscription": subscription.clone() })),
                None => not_found(format!("No EKS Anywhere subscription found for id: {id}.")),
            }
        }
        ("DELETE", [id]) => {
            let id = req_path_value(id);
            match st.eks_anywhere_subscriptions.remove(&id) {
                Some(mut subscription) => {
                    subscription["status"] = json!("DELETING");
                    ok(json!({ "subscription": subscription }))
                }
                None => not_found(format!("No EKS Anywhere subscription found for id: {id}.")),
            }
        }
        ("POST", [id]) => {
            let id = req_path_value(id);
            let Some(subscription) = st.eks_anywhere_subscriptions.get_mut(&id) else {
                return not_found(format!("No EKS Anywhere subscription found for id: {id}."));
            };
            let auto_renew = body
                .get("autoRenew")
                .and_then(Value::as_bool)
                .unwrap_or(true);
            subscription["autoRenew"] = json!(auto_renew);
            ok(json!({ "subscription": subscription.clone() }))
        }
        _ => not_supported_pod(),
    }
}

// ---------------------------------------------------------------------------
// Cluster versions / registrations
// ---------------------------------------------------------------------------

fn cluster_versions_routes(
    req: &Request,
    st: &mut EksStore,
    method: &str,
    rest: &[&str],
    _body: &Value,
) -> Response {
    let _ = st;
    if method != "GET" || !rest.is_empty() {
        return not_supported(req);
    }
    let mut versions: Vec<Value> = CLUSTER_VERSIONS.as_array().unwrap().clone();
    for v in &mut versions {
        v["releaseDate"] = json!(now());
    }
    if let Some(default_only) = query_param(req, "defaultOnly") {
        if default_only == "true" {
            versions.retain(|v| v["defaultVersion"].as_bool().unwrap_or(false));
        }
    }
    if let Some(status) = query_param(req, "status") {
        versions.retain(|v| v["status"].as_str() == Some(status.as_str()));
    }
    if let Some(list) = query_param(req, "clusterVersions") {
        let wanted: Vec<&str> = list.split(',').collect();
        versions.retain(|v| {
            v["clusterVersion"]
                .as_str()
                .is_some_and(|cv| wanted.contains(&cv))
        });
    }
    ok(json!({"clusterVersions": versions, "nextToken": Value::Null}))
}

fn registrations_routes(
    req: &Request,
    st: &mut EksStore,
    method: &str,
    rest: &[&str],
    _body: &Value,
) -> Response {
    match (method, rest) {
        // DeregisterCluster: DELETE /cluster-registrations/{name}
        ("DELETE", [name]) => {
            let name = req_path_value(name);
            if !st.clusters.contains_key(&name) {
                return cluster_not_found(&name);
            }
            let mut record = st.clusters.remove(&name).unwrap();
            record.cluster["status"] = json!("DELETING");
            ok(json!({ "cluster": record.cluster }))
        }
        _ => not_supported(req),
    }
}

// ---------------------------------------------------------------------------
// Tags
// ---------------------------------------------------------------------------

fn tags_routes(
    req: &Request,
    st: &mut EksStore,
    method: &str,
    rest: &[&str],
    body: &Value,
) -> Response {
    let _ = req;
    let arn = percent_decode(&rest.join("/"));
    let not_found_err = || {
        not_found(
            "An error occurred (NotFoundException) when calling the TagResource operation: Resource was not found"
                .to_string(),
        )
    };
    let Some(resource) = find_resource_by_arn(st, &arn) else {
        return not_found_err();
    };
    match method {
        "POST" => {
            let new_tags = body
                .get("tags")
                .and_then(Value::as_object)
                .cloned()
                .unwrap_or_default();
            if !new_tags.is_empty() {
                let tags = resource["tags"].as_object_mut().unwrap();
                for (k, v) in new_tags {
                    tags.insert(k.clone(), v.clone());
                }
            }
            ok(json!({}))
        }
        "DELETE" => {
            // tagKeys are querystring params (repeated `tagKeys=` entries).
            let keys: Vec<String> = req
                .query_string
                .split('&')
                .filter_map(|kv| {
                    let (k, v) = kv.split_once('=').unwrap_or((kv, ""));
                    (k == "tagKeys").then(|| percent_decode(v))
                })
                .collect();
            let tags = resource["tags"].as_object_mut().unwrap();
            for key in keys {
                tags.remove(&key);
            }
            ok(json!({}))
        }
        "GET" => ok(json!({ "tags": resource["tags"].clone() })),
        _ => not_supported_pod(),
    }
}

/// Locate any EKS resource by its ARN (cluster, nodegroup, addon, etc).
fn find_resource_by_arn<'a>(st: &'a mut EksStore, arn: &str) -> Option<&'a mut Value> {
    for record in st.clusters.values_mut() {
        if record.cluster["arn"].as_str() == Some(arn) {
            return Some(&mut record.cluster);
        }
        for ng in record.nodegroups.values_mut() {
            if ng["nodegroupArn"].as_str() == Some(arn) {
                return Some(ng);
            }
        }
        for fp in record.fargate_profiles.values_mut() {
            if fp["fargateProfileArn"].as_str() == Some(arn) {
                return Some(fp);
            }
        }
        for addon in record.addons.values_mut() {
            if addon["addonArn"].as_str() == Some(arn) {
                return Some(addon);
            }
        }
        for entry in record.access_entries.values_mut() {
            if entry["accessEntryArn"].as_str() == Some(arn) {
                return Some(entry);
            }
        }
        for association in record.pod_identity_associations.values_mut() {
            if association["associationArn"].as_str() == Some(arn) {
                return Some(association);
            }
        }
        for config in record.identity_provider_configs.values_mut() {
            if config["identityProviderConfigArn"].as_str() == Some(arn) {
                return Some(config);
            }
        }
        for cap in record.capabilities.values_mut() {
            if cap["arn"].as_str() == Some(arn) {
                return Some(cap);
            }
        }
    }
    st.eks_anywhere_subscriptions
        .values_mut()
        .find(|sub| sub["arn"].as_str() == Some(arn))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    struct Ctx {
        base: Request,
    }

    impl Ctx {
        fn new() -> Self {
            let mut base = Request {
                service: "eks".to_string(),
                operation: String::new(),
                method: "GET".to_string(),
                path: "/".to_string(),
                query_string: String::new(),
                headers: std::collections::HashMap::new(),
                body: bytes::Bytes::new(),
                account: 123456789012,
                region: "us-east-1".to_string(),
            };
            base.body = bytes::Bytes::new();
            Self { base }
        }

        fn call(&self, method: &str, path: &str, params: Value) -> Response {
            let mut req = self.base.clone();
            req.method = method.to_string();
            match path.split_once('?') {
                Some((p, q)) => {
                    req.path = p.to_string();
                    req.query_string = q.to_string();
                }
                None => req.path = path.to_string(),
            }
            req.body = bytes::Bytes::from(serde_json::to_vec(&params).unwrap_or_default());
            dispatch(&req).unwrap()
        }

        fn result(&self, resp: &Response) -> Value {
            serde_json::from_slice(&resp.body).unwrap_or_default()
        }

        fn make_cluster(&self, name: &str) {
            self.call(
                "POST",
                "/clusters",
                json!({
                    "name": name,
                    "roleArn": "arn:aws:iam::123456789012:role/eks-role",
                    "resourcesVpcConfig": {"subnetIds": ["subnet-1"], "securityGroupIds": ["sg-1"]},
                }),
            );
        }
    }

    #[test]
    fn cluster_crud() {
        let ctx = Ctx::new();
        let created = ctx.result(&ctx.call(
            "POST",
            "/clusters",
            json!({
                "name": "c1",
                "roleArn": "arn:aws:iam::123456789012:role/eks-role",
                "resourcesVpcConfig": {"subnetIds": ["subnet-1"], "securityGroupIds": ["sg-1"]},
                "tags": {"env": "test"},
            }),
        ));
        let cluster = &created["cluster"];
        assert_eq!(cluster["name"], "c1");
        assert_eq!(cluster["status"], "ACTIVE");
        assert!(cluster["arn"].as_str().unwrap().ends_with(":cluster/c1"));
        assert!(cluster.get("endpoint").is_some());
        assert!(cluster.get("certificateAuthority").is_some());
        assert!(cluster.get("kubernetesNetworkConfig").is_some());
        assert!(cluster.get("platformVersion").is_some());
        assert!(cluster.get("logging").is_some());
        assert!(cluster.get("createdAt").is_some());
        assert_eq!(cluster["tags"]["env"], "test");

        // Duplicate creation -> ResourceInUseException
        let dup = ctx.call(
            "POST",
            "/clusters",
            json!({
                "name": "c1", "roleArn": "arn:aws:iam::123456789012:role/eks-role",
            }),
        );
        assert_eq!(dup.status, 409);

        // describe
        let desc = ctx.result(&ctx.call("GET", "/clusters/c1", json!({})));
        assert_eq!(desc["cluster"]["name"], "c1");

        // list includes the cluster
        let listed = ctx.result(&ctx.call("GET", "/clusters", json!({})));
        assert!(listed["clusters"]
            .as_array()
            .unwrap()
            .contains(&json!("c1")));

        // delete returns it, then describe raises 404
        let deleted = ctx.result(&ctx.call("DELETE", "/clusters/c1", json!({})));
        assert_eq!(deleted["cluster"]["name"], "c1");
        let resp = ctx.call("GET", "/clusters/c1", json!({}));
        assert_eq!(resp.status, 404);

        // invalid roleArn
        let bad = ctx.call(
            "POST",
            "/clusters",
            json!({"name": "c2", "roleArn": "nope"}),
        );
        assert_eq!(bad.status, 400);
    }

    #[test]
    fn nodegroup_family() {
        let ctx = Ctx::new();
        ctx.make_cluster("cl");
        let created = ctx.result(&ctx.call(
            "POST",
            "/clusters/cl/node-groups",
            json!({
                "nodegroupName": "ng1",
                "nodeRole": "arn:aws:iam::123456789012:role/node-role",
                "subnets": ["subnet-1"],
                "scalingConfig": {"minSize": 1, "maxSize": 3, "desiredSize": 2},
                "tags": {"env": "test"},
            }),
        ));
        let ng = &created["nodegroup"];
        assert_eq!(ng["nodegroupName"], "ng1");
        assert_eq!(ng["clusterName"], "cl");
        assert!(ng["nodegroupArn"]
            .as_str()
            .unwrap()
            .contains("nodegroup/cl/ng1"));
        assert_eq!(ng["scalingConfig"]["maxSize"], 3);
        assert_eq!(ng["tags"]["env"], "test");

        // duplicate -> 409
        let dup = ctx.call(
            "POST",
            "/clusters/cl/node-groups",
            json!({
                "nodegroupName": "ng1",
                "nodeRole": "arn:aws:iam::123456789012:role/node-role",
            }),
        );
        assert_eq!(dup.status, 409);

        // describe
        let desc = ctx.result(&ctx.call("GET", "/clusters/cl/node-groups/ng1", json!({})));
        assert_eq!(
            desc["nodegroup"]["nodeRole"],
            "arn:aws:iam::123456789012:role/node-role"
        );

        // list
        let listed = ctx.result(&ctx.call("GET", "/clusters/cl/node-groups", json!({})));
        assert!(listed["nodegroups"]
            .as_array()
            .unwrap()
            .contains(&json!("ng1")));

        // update config -> change sizes
        let updated = ctx.result(&ctx.call(
            "POST",
            "/clusters/cl/node-groups/ng1/update-config",
            json!({"scalingConfig": {"minSize": 1, "maxSize": 5, "desiredSize": 3}}),
        ));
        assert_eq!(updated["update"]["type"], "ConfigUpdate");
        let desc = ctx.result(&ctx.call("GET", "/clusters/cl/node-groups/ng1", json!({})));
        assert_eq!(desc["nodegroup"]["scalingConfig"]["maxSize"], 5);

        // unknown nodegroup describe -> 404
        let resp = ctx.call("GET", "/clusters/cl/node-groups/nope", json!({}));
        assert_eq!(resp.status, 404);

        // delete nodegroup; cluster delete now allowed
        let deleted = ctx.result(&ctx.call("DELETE", "/clusters/cl/node-groups/ng1", json!({})));
        assert_eq!(deleted["nodegroup"]["nodegroupName"], "ng1");
        // cluster in use -> 409 when nodegroups remain
        ctx.call(
            "POST",
            "/clusters/cl/node-groups",
            json!({
                "nodegroupName": "ng2", "nodeRole": "arn:aws:iam::123456789012:role/node-role",
            }),
        );
        let resp = ctx.call("DELETE", "/clusters/cl", json!({}));
        assert_eq!(resp.status, 409);
    }

    #[test]
    fn fargate_addons_pod_identity_access_entries() {
        let ctx = Ctx::new();
        ctx.make_cluster("cl");

        // fargate profile
        let created = ctx.result(&ctx.call(
            "POST",
            "/clusters/cl/fargate-profiles",
            json!({
                "fargateProfileName": "fp1",
                "podExecutionRoleArn": "arn:aws:iam::123456789012:role/fp",
                "selectors": [{"namespace": "default"}, {"namespace": "kube-system"}],
            }),
        ));
        let fp = &created["fargateProfile"];
        assert_eq!(fp["fargateProfileName"], "fp1");
        assert_eq!(fp["clusterName"], "cl");
        assert!(fp.get("status").is_some());
        assert_eq!(fp["selectors"].as_array().unwrap().len(), 2);
        let listed = ctx.result(&ctx.call("GET", "/clusters/cl/fargate-profiles", json!({})));
        assert!(listed["fargateProfileNames"]
            .as_array()
            .unwrap()
            .contains(&json!("fp1")));
        let desc = ctx.result(&ctx.call("GET", "/clusters/cl/fargate-profiles/fp1", json!({})));
        assert_eq!(
            desc["fargateProfile"]["fargateProfileArn"],
            fp["fargateProfileArn"]
        );
        let deleted =
            ctx.result(&ctx.call("DELETE", "/clusters/cl/fargate-profiles/fp1", json!({})));
        assert_eq!(deleted["fargateProfile"]["fargateProfileName"], "fp1");
        let resp = ctx.call("GET", "/clusters/cl/fargate-profiles/fp1", json!({}));
        assert_eq!(resp.status, 404);

        // addons
        let addon = ctx.result(&ctx.call(
            "POST",
            "/clusters/cl/addons",
            json!({"addonName": "vpc-cni"}),
        ));
        assert_eq!(addon["addon"]["addonName"], "vpc-cni");
        assert!(addon["addon"]["addonArn"]
            .as_str()
            .unwrap()
            .contains(":addon/cl/vpc-cni"));
        let listed = ctx.result(&ctx.call("GET", "/clusters/cl/addons", json!({})));
        assert!(listed["addons"]
            .as_array()
            .unwrap()
            .contains(&json!("vpc-cni")));
        let updated = ctx.result(&ctx.call(
            "POST",
            "/clusters/cl/addons/vpc-cni/update",
            json!({"addonVersion": "v2"}),
        ));
        assert_eq!(updated["update"]["type"], "AddonUpdate");
        let deleted = ctx.result(&ctx.call("DELETE", "/clusters/cl/addons/vpc-cni", json!({})));
        assert_eq!(deleted["addon"]["status"], "DELETING");
        let versions = ctx.result(&ctx.call("GET", "/addons/supported-versions", json!({})));
        assert!(!versions["addons"].as_array().unwrap().is_empty());

        // pod identity association
        let assoc = ctx.result(&ctx.call(
            "POST",
            "/clusters/cl/pod-identity-associations",
            json!({
                "namespace": "kube-system",
                "serviceAccount": "sa",
                "roleArn": "arn:aws:iam::123456789012:role/pod",
            }),
        ));
        let association_id = assoc["association"]["associationId"]
            .as_str()
            .unwrap()
            .to_string();
        assert!(association_id.starts_with("a-"));
        let listed =
            ctx.result(&ctx.call("GET", "/clusters/cl/pod-identity-associations", json!({})));
        assert!(listed["associations"]
            .as_array()
            .unwrap()
            .iter()
            .any(|a| a["associationId"] == association_id));
        let desc = ctx.result(&ctx.call(
            "GET",
            &format!("/clusters/cl/pod-identity-associations/{association_id}"),
            json!({}),
        ));
        assert_eq!(desc["association"]["namespace"], "kube-system");
        let deleted = ctx.result(&ctx.call(
            "DELETE",
            &format!("/clusters/cl/pod-identity-associations/{association_id}"),
            json!({}),
        ));
        assert_eq!(deleted["association"]["associationId"], association_id);
        let resp = ctx.call(
            "GET",
            &format!("/clusters/cl/pod-identity-associations/{association_id}"),
            json!({}),
        );
        assert_eq!(resp.status, 404);

        // access entries
        let entry = ctx.result(&ctx.call(
            "POST",
            "/clusters/cl/access-entries",
            json!({
                "principalArn": "arn:aws:iam::123456789012:role/access-role",
            }),
        ));
        assert_eq!(entry["accessEntry"]["clusterName"], "cl");
        assert!(entry["accessEntry"]["accessEntryArn"].is_string());
        let listed = ctx.result(&ctx.call("GET", "/clusters/cl/access-entries", json!({})));
        assert!(listed["accessEntries"]
            .as_array()
            .unwrap()
            .contains(&json!("arn:aws:iam::123456789012:role/access-role")));

        // catalog + policies + versions
        let versions = ctx.result(&ctx.call("GET", "/cluster-versions", json!({})));
        assert!(versions["clusterVersions"].as_array().unwrap().len() >= 4);
        let policies = ctx.result(&ctx.call("GET", "/access-policies", json!({})));
        assert!(!policies["accessPolicies"].as_array().unwrap().is_empty());
    }

    #[test]
    fn capabilities_idp_insights_subscriptions_and_tags() {
        let ctx = Ctx::new();
        ctx.make_cluster("cl");

        // capabilities
        let created = ctx.result(&ctx.call(
            "POST",
            "/clusters/cl/capabilities",
            json!({
                "capabilityName": "cap",
                "type": "ACK",
                "roleArn": "arn:aws:iam::123456789012:role/cap",
                "deletePropagationPolicy": "RETAIN",
            }),
        ));
        let cap = &created["capability"];
        assert_eq!(cap["capabilityName"], "cap");
        assert_eq!(cap["status"], "ACTIVE");
        assert_eq!(cap["deletePropagationPolicy"], "RETAIN");
        let updated = ctx.result(&ctx.call(
            "POST",
            "/clusters/cl/capabilities/cap",
            json!({"roleArn": "arn:aws:iam::123456789012:role/new-cap"}),
        ));
        assert_eq!(updated["update"]["type"], "CapabilityUpdate");
        assert_eq!(updated["update"]["status"], "Successful");
        let desc = ctx.result(&ctx.call("GET", "/clusters/cl/capabilities/cap", json!({})));
        assert_eq!(desc["capability"]["capabilityName"], "cap");
        let deleted = ctx.result(&ctx.call("DELETE", "/clusters/cl/capabilities/cap", json!({})));
        assert_eq!(deleted["capability"]["capabilityName"], "cap");

        // identity provider config
        let associated = ctx.result(&ctx.call(
            "POST",
            "/clusters/cl/identity-provider-configs/associate",
            json!({"oidc": {"identityProviderConfigName": "oidc1", "issuerUrl": "https://x", "clientId": "cid"}}),
        ));
        assert!(!associated["update"]["id"].as_str().unwrap().is_empty());
        let listed =
            ctx.result(&ctx.call("GET", "/clusters/cl/identity-provider-configs", json!({})));
        assert!(!listed["identityProviderConfigs"]
            .as_array()
            .unwrap()
            .is_empty());
        let described = ctx.result(&ctx.call(
            "POST",
            "/clusters/cl/identity-provider-configs/describe",
            json!({"identityProviderConfig": {"type": "oidc", "name": "oidc1"}}),
        ));
        assert_eq!(
            described["identityProviderConfig"]["oidc"]["identityProviderConfigName"],
            "oidc1"
        );
        let disassociated = ctx.result(&ctx.call(
            "POST",
            "/clusters/cl/identity-provider-configs/disassociate",
            json!({"identityProviderConfig": {"type": "oidc", "name": "oidc1"}}),
        ));
        assert!(disassociated["update"].is_object());

        // insights: empty list, fake id 404, refresh completes
        let listed = ctx.result(&ctx.call("POST", "/clusters/cl/insights", json!({})));
        assert!(listed["insights"].as_array().unwrap().is_empty());
        let resp = ctx.call("GET", "/clusters/cl/insights/fake-id", json!({}));
        assert_eq!(resp.status, 404);
        let refresh = ctx.result(&ctx.call("POST", "/clusters/cl/insights-refresh", json!({})));
        assert_eq!(refresh["status"], "COMPLETED");
        let described = ctx.result(&ctx.call("GET", "/clusters/cl/insights-refresh", json!({})));
        assert_eq!(described["status"], "COMPLETED");

        // subscriptions
        let created = ctx.result(&ctx.call(
            "POST",
            "/eks-anywhere-subscriptions",
            json!({
                "name": "sub1",
                "term": {"duration": 1, "unit": "MONTHS"},
            }),
        ));
        let sub = &created["subscription"];
        assert_eq!(sub["status"], "ACTIVE");
        assert!(sub["id"].is_string());
        let sub_id = sub["id"].as_str().unwrap().to_string();
        let described = ctx.result(&ctx.call(
            "GET",
            &format!("/eks-anywhere-subscriptions/{sub_id}"),
            json!({}),
        ));
        assert_eq!(described["subscription"]["id"], sub_id);
        let updated = ctx.result(&ctx.call(
            "POST",
            &format!("/eks-anywhere-subscriptions/{sub_id}"),
            json!({"autoRenew": false}),
        ));
        assert_eq!(updated["subscription"]["autoRenew"], false);
        let deleted = ctx.result(&ctx.call(
            "DELETE",
            &format!("/eks-anywhere-subscriptions/{sub_id}"),
            json!({}),
        ));
        assert_eq!(deleted["subscription"]["id"], sub_id);

        // tags via cluster arn
        let cluster = ctx.result(&ctx.call("GET", "/clusters/cl", json!({})))["cluster"].clone();
        let arn = cluster["arn"].as_str().unwrap().to_string();
        ctx.call(
            "POST",
            &format!("/tags/{}", urlencode(&arn)),
            json!({"tags": {"team": "platform"}}),
        );
        let tags = ctx.result(&ctx.call("GET", &format!("/tags/{}", urlencode(&arn)), json!({})));
        assert_eq!(tags["tags"]["team"], "platform");
        ctx.call(
            "DELETE",
            &format!("/tags/{}?tagKeys=team", urlencode(&arn)),
            json!({}),
        );
        let tags = ctx.result(&ctx.call("GET", &format!("/tags/{}", urlencode(&arn)), json!({})));
        assert!(tags["tags"]
            .as_object()
            .map(|t| t.is_empty())
            .unwrap_or(false));

        // deregister nonexistent cluster
        let resp = ctx.call("DELETE", "/cluster-registrations/nope", json!({}));
        assert_eq!(resp.status, 404);
    }

    fn urlencode(s: &str) -> String {
        let mut out = String::new();
        for b in s.bytes() {
            match b {
                b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b':' | b'/' | b'-' | b'_' | b'.' => {
                    out.push(b as char)
                }
                _ => out.push_str(&format!("%{b:02X}")),
            }
        }
        out
    }
}
