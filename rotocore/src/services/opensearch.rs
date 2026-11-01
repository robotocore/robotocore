//! OpenSearch + Elasticsearch backend (rest-json; serves both `opensearch`
//! and `es` service names). Rest-json derives the operation from
//! method+path; errors use proto::json_error (non-2xx `__type` codes).

use std::collections::HashMap;

use crate::proto::{json_error, parse_json_body};
use crate::store::{self, Slot};
use crate::{Request, Response};
use serde_json::{json, Map, Value};

/// Domain config keys echoed verbatim from create/update calls.
const OPTION_KEYS: &[&str] = &[
    "AdvancedOptions",
    "AdvancedSecurityOptions",
    "AutoTuneOptions",
    "ClusterConfig",
    "CognitoOptions",
    "DomainEndpointOptions",
    "EBSOptions",
    "EncryptionAtRestOptions",
    "LogPublishingOptions",
    "NodeToNodeEncryptionOptions",
    "OffPeakWindowOptions",
    "SnapshotOptions",
    "SoftwareUpdateOptions",
];

/// Canonical reserved-instance offering id served by describes.
const OFFERING_ID: &str = "os-offering-1";

fn ok_json(value: Value) -> Response {
    Response::json_status(200, &value)
}

fn err_json(status: u16, code: &str, message: &str) -> Response {
    json_error(status, code, message)
}

fn not_found(resource: String) -> Response {
    err_json(
        400,
        "ResourceNotFoundException",
        &format!("{resource} not found"),
    )
}

/// moto's unimplemented-op contract: an unmodeled URI answers `404 ""`
/// (`core/responses.py::call_action` — no action resolves) and a modeled op
/// outside the table answers the repo's 501 NotImplemented. The old
/// `200 {}` fall-through masked gaps as successes, breaking the.
/// 501-is-the-only-absence-signal contract the twin's audit/introspection
/// layers read.
fn unknown_uri() -> Response {
    Response::json_status(404, &serde_json::json!({}))
}

fn not_implemented(op: &str) -> Response {
    err_json(
        501,
        "NotImplemented",
        &format!("The {} action has not been implemented", op),
    )
}

/// Full ServiceSoftwareOptions shape (moto's Domain.software_options).
fn software_options() -> Value {
    json!({
        "CurrentVersion": "",
        "NewVersion": "",
        "UpdateAvailable": false,
        "Cancellable": false,
        "UpdateStatus": "COMPLETED",
        "Description": "There is no software update available for this domain.",
        "AutomatedUpdateDate": "1969-12-31T23:00:00-01:00",
        "OptionalDeployment": true
    })
}

#[derive(Clone)]
pub struct Domain {
    name: String,
    engine_version: String,
    vpc: bool,
    subnet_ids: Vec<String>,
    access_policies: String,
    options: Map<String, Value>,
    tags: HashMap<String, String>,
}

#[derive(Default)]
pub struct OpenSearchState {
    domains: Vec<Domain>,
    packages: Vec<OsPackage>,
    /// domain name → associated package ids
    domain_packages: HashMap<String, Vec<String>>,
    /// (domain name, data source name) → data source dict
    data_sources: HashMap<(String, String), Value>,
    /// domain name → maintenance records
    domain_maintenances: HashMap<String, Vec<Value>>,
    /// VPC endpoint id → endpoint dict
    vpc_endpoints: HashMap<String, Value>,
}

struct OsPackage {
    id: String,
    name: String,
    kind: String,
    description: String,
}

pub fn dispatch(req: &Request) -> Response {
    let body = parse_json_body(&req.body);
    let op = derive_op(&req.method, &req.path);
    if op.is_empty() {
        return unknown_uri();
    }
    handle(req, &body, &op)
}

fn with_scope<T>(
    slot: &Slot<OpenSearchState>,
    req: &Request,
    f: impl FnOnce(&mut OpenSearchState) -> T,
) -> T {
    let mut map = slot.write();
    f(map.entry((req.account, req.region.clone())).or_default())
}

fn rand_hex(len: usize) -> String {
    const HEX: &[u8] = b"0123456789abcdef";
    let mut out = String::with_capacity(len);
    while out.len() < len {
        for b in uuid::Uuid::new_v4().as_bytes() {
            if out.len() == len {
                break;
            }
            out.push(HEX[(*b % 16) as usize] as char);
        }
    }
    out
}

/// Strip the version segment (e.g. `2021-01-01`) plus opensearch/es markers.
fn strip_version_marker(path: &str) -> Vec<String> {
    let p = path.trim_start_matches('/').trim_end_matches('/');
    let mut out: Vec<String> = p.split('/').map(|x| x.to_string()).collect();
    if !out.is_empty() && out[0].chars().all(|c| c.is_ascii_digit() || c == '-') {
        out.remove(0);
    }
    for marker in ["opensearch", "es"] {
        if out.first().map(String::as_str) == Some(marker) {
            out.remove(0);
        }
    }
    out.into_iter().filter(|x| !x.is_empty()).collect()
}

fn derive_op(method: &str, path: &str) -> String {
    let req_segs = strip_version_marker(path);
    let req_refs: Vec<&str> = req_segs.iter().map(String::as_str).collect();
    for (method_spec, path_spec, op) in op_table() {
        if method_spec != method {
            continue;
        }
        let spec_segs: Vec<&str> = path_spec.split('/').filter(|x| !x.is_empty()).collect();
        if segments_match(&spec_segs, &req_refs) {
            return op.to_string();
        }
    }
    String::new()
}

/// Segments equal except `{Param}` matches any single non-empty segment.
fn segments_match(spec: &[&str], req: &[&str]) -> bool {
    spec.len() == req.len()
        && spec
            .iter()
            .zip(req.iter())
            .all(|(s, r)| (s.starts_with('{') && s.ends_with('}')) || s == r)
}

/// `(method, segments, op)` specs from the botocore opensearch/es models.
/// `{Param}` matches one non-empty segment; both client flavors strip to the
/// same specs. Client-flavored response keys branch on `req.service`.
fn op_table() -> Vec<(&'static str, &'static str, &'static str)> {
    vec![
        ("POST", "tags", "AddTags"),
        ("POST", "tags-removal", "RemoveTags"),
        ("GET", "tags", "ListTags"),
        ("GET", "domain", "ListDomainNames"),
        ("POST", "domain", "CreateDomain"),
        ("PUT", "domain/{DomainName}", "CreateDomain"),
        ("DELETE", "domain/{DomainName}", "DeleteDomain"),
        ("GET", "domain/{DomainName}", "DescribeDomain"),
        ("POST", "domain-info", "DescribeDomains"),
        ("GET", "domain/{DomainName}/config", "DescribeDomainConfig"),
        ("POST", "domain/{DomainName}/config", "UpdateDomainConfig"),
        (
            "POST",
            "domain/{DomainName}/config/cancel",
            "CancelDomainConfigChange",
        ),
        ("GET", "domain/{DomainName}/health", "DescribeDomainHealth"),
        ("GET", "domain/{DomainName}/nodes", "DescribeDomainNodes"),
        (
            "GET",
            "domain/{DomainName}/progress",
            "DescribeDomainChangeProgress",
        ),
        (
            "GET",
            "domain/{DomainName}/autoTunes",
            "DescribeDomainAutoTunes",
        ),
        (
            "GET",
            "domain/{DomainName}/dryRun",
            "DescribeDryRunProgress",
        ),
        ("GET", "versions", "ListVersions"),
        ("GET", "compatibleVersions", "GetCompatibleVersions"),
        ("POST", "packages", "CreatePackage"),
        ("POST", "packages/describe", "DescribePackages"),
        (
            "POST",
            "packages/associate/{PackageID}/{DomainName}",
            "AssociatePackage",
        ),
        (
            "POST",
            "packages/dissociate/{PackageID}/{DomainName}",
            "DissociatePackage",
        ),
        ("POST", "packages/update", "UpdatePackage"),
        ("DELETE", "packages/{PackageID}", "DeletePackage"),
        (
            "GET",
            "packages/{PackageID}/domains",
            "ListDomainsForPackage",
        ),
        (
            "GET",
            "packages/{PackageID}/history",
            "GetPackageVersionHistory",
        ),
        (
            "GET",
            "domain/{DomainName}/packages",
            "ListPackagesForDomain",
        ),
        ("POST", "vpcEndpoints", "CreateVpcEndpoint"),
        ("POST", "vpcEndpoints/describe", "DescribeVpcEndpoints"),
        ("POST", "vpcEndpoints/update", "UpdateVpcEndpoint"),
        (
            "DELETE",
            "vpcEndpoints/{VpcEndpointId}",
            "DeleteVpcEndpoint",
        ),
        ("GET", "vpcEndpoints", "ListVpcEndpoints"),
        (
            "GET",
            "domain/{DomainName}/vpcEndpoints",
            "ListVpcEndpointsForDomain",
        ),
        (
            "POST",
            "domain/{DomainName}/authorizeVpcEndpointAccess",
            "AuthorizeVpcEndpointAccess",
        ),
        (
            "POST",
            "domain/{DomainName}/revokeVpcEndpointAccess",
            "RevokeVpcEndpointAccess",
        ),
        (
            "GET",
            "domain/{DomainName}/listVpcEndpointAccess",
            "ListVpcEndpointAccess",
        ),
        ("POST", "cc/outboundConnection", "CreateOutboundConnection"),
        (
            "POST",
            "cc/inboundConnection/search",
            "DescribeInboundConnections",
        ),
        (
            "POST",
            "cc/outboundConnection/search",
            "DescribeOutboundConnections",
        ),
        (
            "PUT",
            "cc/inboundConnection/{ConnectionId}/accept",
            "AcceptInboundConnection",
        ),
        (
            "DELETE",
            "cc/inboundConnection/{ConnectionId}",
            "DeleteInboundConnection",
        ),
        (
            "DELETE",
            "cc/outboundConnection/{ConnectionId}",
            "DeleteOutboundConnection",
        ),
        ("POST", "ccs/outboundConnection", "CreateOutboundConnection"),
        (
            "POST",
            "ccs/inboundConnection/search",
            "DescribeInboundConnections",
        ),
        (
            "POST",
            "ccs/outboundConnection/search",
            "DescribeOutboundConnections",
        ),
        (
            "PUT",
            "ccs/inboundConnection/{ConnectionId}/accept",
            "AcceptInboundConnection",
        ),
        (
            "PUT",
            "ccs/inboundConnection/{ConnectionId}/reject",
            "RejectInboundConnection",
        ),
        (
            "DELETE",
            "ccs/inboundConnection/{ConnectionId}",
            "DeleteInboundConnection",
        ),
        (
            "DELETE",
            "ccs/outboundConnection/{ConnectionId}",
            "DeleteOutboundConnection",
        ),
        ("DELETE", "role", "DeleteVpcServiceRole"),
        (
            "GET",
            "reservedInstanceOfferings",
            "DescribeReservedInstanceOfferings",
        ),
        ("GET", "reservedInstances", "DescribeReservedInstances"),
        (
            "POST",
            "purchaseReservedInstanceOffering",
            "PurchaseReservedInstanceOffering",
        ),
        (
            "POST",
            "serviceSoftwareUpdate/start",
            "StartServiceSoftwareUpdate",
        ),
        (
            "POST",
            "serviceSoftwareUpdate/cancel",
            "CancelServiceSoftwareUpdate",
        ),
        ("POST", "upgradeDomain", "UpgradeDomain"),
        (
            "GET",
            "upgradeDomain/{DomainName}/history",
            "GetUpgradeHistory",
        ),
        (
            "GET",
            "upgradeDomain/{DomainName}/status",
            "GetUpgradeStatus",
        ),
        (
            "GET",
            "instanceTypeDetails/{EngineVersion}",
            "ListInstanceTypeDetails",
        ),
        (
            "GET",
            "instanceTypeLimits/{EngineVersion}/{InstanceType}",
            "DescribeInstanceTypeLimits",
        ),
        (
            "GET",
            "instanceTypes/{ElasticsearchVersion}",
            "ListInstanceTypeDetails",
        ),
        ("POST", "domain/{DomainName}/dataSource", "AddDataSource"),
        ("GET", "domain/{DomainName}/dataSource", "ListDataSources"),
        (
            "GET",
            "domain/{DomainName}/dataSource/{DataSourceName}",
            "GetDataSource",
        ),
        (
            "PUT",
            "domain/{DomainName}/dataSource/{DataSourceName}",
            "UpdateDataSource",
        ),
        (
            "DELETE",
            "domain/{DomainName}/dataSource/{DataSourceName}",
            "DeleteDataSource",
        ),
        (
            "GET",
            "directQueryDataSource/{DataSourceName}",
            "GetDirectQueryDataSource",
        ),
        (
            "DELETE",
            "directQueryDataSource/{DataSourceName}",
            "DeleteDirectQueryDataSource",
        ),
        (
            "POST",
            "domain/{DomainName}/domainMaintenance",
            "StartDomainMaintenance",
        ),
        (
            "GET",
            "domain/{DomainName}/domainMaintenance",
            "GetDomainMaintenanceStatus",
        ),
        (
            "GET",
            "domain/{DomainName}/domainMaintenances",
            "ListDomainMaintenances",
        ),
        (
            "GET",
            "domain/{DomainName}/scheduledActions",
            "ListScheduledActions",
        ),
        (
            "PUT",
            "domain/{DomainName}/scheduledAction/update",
            "UpdateScheduledAction",
        ),
        ("GET", "list-applications", "ListApplications"),
        ("POST", "application", "CreateApplication"),
        ("GET", "application/{id}", "GetApplication"),
        ("PUT", "application/{id}", "UpdateApplication"),
        ("DELETE", "application/{id}", "DeleteApplication"),
        (
            "GET",
            "defaultApplicationSetting",
            "GetDefaultApplicationSetting",
        ),
        (
            "PUT",
            "defaultApplicationSetting",
            "PutDefaultApplicationSetting",
        ),
    ]
}

fn urldecode(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() + 1 && i + 2 < bytes.len() => {
                let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or("25");
                out.push(u8::from_str_radix(hex, 16).unwrap_or(b'%'));
                i += 3;
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn query_param(query: &str, key: &str) -> String {
    query
        .split('&')
        .filter_map(|kv| kv.split_once('='))
        .find(|(k, _)| *k == key)
        .map(|(_, v)| urldecode(v))
        .unwrap_or_default()
}

fn path_domain(path: &str) -> String {
    let segs: Vec<&str> = path.split('/').collect();
    match segs.iter().position(|s| *s == "domain") {
        Some(pos) => segs.get(pos + 1).map(|s| s.to_string()).unwrap_or_default(),
        None => String::new(),
    }
}

pub fn handle(req: &Request, body: &Value, op: &str) -> Response {
    let slot = store::slot::<OpenSearchState>("opensearch");
    match op {
        "ListDomainNames" => {
            let list: Vec<Value> = with_scope(&slot, req, |st| {
                st.domains.iter().map(|d| d.name_summary()).collect()
            });
            ok_json(json!({ "DomainNames": list }))
        }
        "ListVersions" => {
            if req.service == "es" {
                ok_json(
                    json!({ "ElasticsearchVersions": [ "7.10", "7.9", "7.8", "7.7", "7.4", "7.1", "6.8", "6.5", "6.0", "5.6", "5.5", "5.1" ] }),
                )
            } else {
                ok_json(
                    json!({ "Versions": [ "OpenSearch_2.13", "OpenSearch_2.11", "OpenSearch_2.9", "OpenSearch_2.5", "OpenSearch_2.3", "OpenSearch_1.3", "OpenSearch_1.1", "OpenSearch_1.0", "Elasticsearch_7.10", "Elasticsearch_7.9", "Elasticsearch_7.4", "Elasticsearch_7.1", "Elasticsearch_6.8", "Elasticsearch_6.5", "Elasticsearch_5.6", "Elasticsearch_5.1" ] }),
                )
            }
        }
        "CreateDomain" => {
            let mut name = body_field(body, "DomainName");
            if name.is_empty() {
                name = path_domain(&req.path);
            }
            let exists = with_scope(&slot, req, |st| st.domains.iter().any(|d| d.name == name));
            if exists {
                return err_json(
                    409,
                    "ResourceAlreadyExistsException",
                    &format!("Domain {name} already exists"),
                );
            }
            let version = body
                .get("EngineVersion")
                .or_else(|| body.get("ElasticsearchVersion"))
                .and_then(Value::as_str)
                .unwrap_or("OpenSearch_2.5")
                .to_string();
            let mut options = Map::new();
            for key in OPTION_KEYS {
                if let Some(v) = body.get(*key) {
                    options.insert((*key).to_string(), v.clone());
                }
            }
            if let Some(cfg) = body
                .get("ClusterConfig")
                .or_else(|| body.get("ElasticsearchClusterConfig"))
                .cloned()
            {
                options.insert("ClusterConfig".into(), cfg);
            }
            let vpc = body.get("VPCOptions").is_some();
            let subnet_ids = body
                .get("VPCOptions")
                .and_then(|k| k.get("SubnetIds"))
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(|x| x.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default();
            let domain = Domain {
                name: name.clone(),
                engine_version: version,
                vpc,
                subnet_ids,
                options,
                access_policies: body_field(body, "AccessPolicies"),
                tags: HashMap::new(),
            };
            let status = domain.status(req.account, &req.region);
            with_scope(&slot, req, |st| st.domains.push(domain));
            ok_json(json!({ "DomainStatus": status }))
        }
        "DescribeDomain" => {
            let name = name_of(req, body);
            let status = with_scope(&slot, req, |st| {
                st.domains
                    .iter()
                    .find(|d| d.name == name)
                    .map(|d| d.status(req.account, &req.region))
            });
            match status {
                Some(s) => ok_json(json!({ "DomainStatus": s })),
                None => not_found(format!("Domain {name}")),
            }
        }
        "DescribeDomains" => {
            let names: Vec<String> = body
                .get("DomainNames")
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(|x| x.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default();
            let list: Vec<Value> = with_scope(&slot, req, |st| {
                names
                    .iter()
                    .filter_map(|n| st.domains.iter().find(|d| d.name == *n))
                    .map(|d| d.status(req.account, &req.region))
                    .collect()
            });
            ok_json(json!({ "DomainStatusList": list }))
        }
        "DescribeDomainConfig" => {
            let name = name_of(req, body);
            let cfg = with_scope(&slot, req, |st| {
                st.domains
                    .iter()
                    .find(|d| d.name == name)
                    .map(|d| d.config_status(req.account, &req.region))
            });
            match cfg {
                Some(c) => ok_json(c),
                None => not_found(format!("Domain {name}")),
            }
        }
        "UpdateDomainConfig" => {
            let name = name_of(req, body);
            let mut cfg: Option<Value> = None;
            with_scope(&slot, req, |st| {
                if let Some(d) = st.domains.iter_mut().find(|d| d.name == name) {
                    for key in OPTION_KEYS {
                        if let Some(v) = body.get(*key) {
                            d.options.insert((*key).to_string(), v.clone());
                        }
                    }
                    if let Some(v) = body
                        .get("ClusterConfig")
                        .or_else(|| body.get("ElasticsearchClusterConfig"))
                        .cloned()
                    {
                        d.options.insert("ClusterConfig".into(), v);
                    }
                    if let Some(v) = body
                        .get("EngineVersion")
                        .or_else(|| body.get("ElasticsearchVersion"))
                        .and_then(Value::as_str)
                    {
                        d.engine_version = v.to_string();
                    }
                    if let Some(v) = body.get("AccessPolicies").and_then(Value::as_str) {
                        d.access_policies = v.to_string();
                    }
                    cfg = Some(d.config_status(req.account, &req.region));
                }
            });
            match cfg {
                Some(c) => ok_json(c),
                None => not_found(format!("Domain {name}")),
            }
        }
        "DeleteDomain" => {
            let name = name_of(req, body);
            let mut status: Option<Value> = None;
            with_scope(&slot, req, |st| {
                if let Some(pos) = st.domains.iter().position(|d| d.name == name) {
                    let d = st.domains.remove(pos);
                    let mut s = d.status(req.account, &req.region);
                    s["Deleted"] = json!(true);
                    status = Some(s);
                }
            });
            match status {
                Some(s) => ok_json(json!({ "DomainStatus": s })),
                None => not_found(format!("Domain {name}")),
            }
        }
        "DescribeDomainHealth" => {
            let name = path_domain(&req.path);
            let exists = with_scope(&slot, req, |st| st.domains.iter().any(|d| d.name == name));
            if !exists {
                return not_found(format!("Domain {name}"));
            }
            ok_json(json!({
                "DomainState": "Active", "AvailabilityZoneCount": "1",
                "ActiveAvailabilityZoneCount": "1", "StandByAvailabilityZoneCount": "0",
                "DataNodeCount": "1", "DedicatedMaster": false, "MasterEligibleNodeCount": "0",
                "WarmNodeCount": "0", "ClusterHealth": "Green", "TotalShards": "1",
                "TotalUnAssignedShards": "0", "InstanceCount": 1, "ZoneCount": 1
            }))
        }
        "DescribeDomainNodes" => {
            let name = path_domain(&req.path);
            let exists = with_scope(&slot, req, |st| st.domains.iter().any(|d| d.name == name));
            if !exists {
                return not_found(format!("Domain {name}"));
            }
            ok_json(
                json!({ "DomainNodesStatusList": [{ "NodeId": "node-0", "NodeType": "Data",
                "AvailabilityZone": format!("{}a", req.region), "InstanceType": "t3.small.search",
                "NodeStatus": "Active", "StorageType": "EBS", "StorageVolumeType": "gp2", "StorageSize": "10" }] }),
            )
        }
        "DescribeDomainChangeProgress" => {
            let name = path_domain(&req.path);
            let exists = with_scope(&slot, req, |st| st.domains.iter().any(|d| d.name == name));
            if !exists {
                return not_found(format!("Domain {name}"));
            }
            ok_json(
                json!({ "ChangeProgressStatus": { "ChangeId": "change-id-placeholder",
                "StartTime": chrono::Utc::now().format("%Y-%m-%d %H:%M:%S%.6f+00:00").to_string(),
                "Status": "COMPLETED", "PendingProperties": [], "CompletedProperties": [],
                "TotalNumberOfStages": 1, "ChangeProgressStages": [{ "Name": "Done", "Status": "COMPLETED" }] } }),
            )
        }
        "DescribeDomainAutoTunes" => {
            let name = path_domain(&req.path);
            let exists = with_scope(&slot, req, |st| st.domains.iter().any(|d| d.name == name));
            if !exists {
                return not_found(format!("Domain {name}"));
            }
            ok_json(json!({ "AutoTunes": [] }))
        }
        "DescribeDryRunProgress" => {
            let name = path_domain(&req.path);
            let exists = with_scope(&slot, req, |st| st.domains.iter().any(|d| d.name == name));
            if !exists {
                return not_found(format!("Domain {name}"));
            }
            ok_json(
                json!({ "DryRunProgressStatus": { "ProgressStatus": "COMPLETED",
                "CreationDate": chrono::Utc::now().to_rfc3339() }, "DryRunConfig": {}, "DryRunResults": {} }),
            )
        }
        "GetCompatibleVersions" => {
            if let Some(n) = body.get("DomainName").and_then(Value::as_str) {
                let exists = with_scope(&slot, req, |st| st.domains.iter().any(|d| d.name == n));
                if !exists {
                    return not_found(format!("Domain {n}"));
                }
            }
            let entries = if req.service == "es" {
                vec![
                    json!({ "SourceVersion": "6.8", "TargetVersions": ["7.0", "7.1", "7.4", "7.9", "7.10"] }),
                    json!({ "SourceVersion": "7.1", "TargetVersions": ["7.4", "7.9", "7.10"] }),
                    json!({ "SourceVersion": "7.4", "TargetVersions": ["7.9", "7.10"] }),
                ]
            } else {
                vec![
                    json!({ "SourceVersion": "OpenSearch_1.3", "TargetVersions": ["OpenSearch_2.3", "OpenSearch_2.5"] }),
                    json!({ "SourceVersion": "OpenSearch_2.5", "TargetVersions": ["OpenSearch_2.9", "OpenSearch_2.11"] }),
                ]
            };
            let key = if req.service == "es" {
                "CompatibleElasticsearchVersions"
            } else {
                "CompatibleVersions"
            };
            let mut out = Map::new();
            out.insert(key.to_string(), Value::Array(entries));
            ok_json(Value::Object(out))
        }
        "CancelDomainConfigChange" => {
            ok_json(json!({ "CancelledChangeIds": [], "CancelledProperties": [] }))
        }
        "AddTags" => {
            let arn = body_field(body, "ARN");
            let tags: Vec<(String, String)> = body
                .get("TagList")
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(|t| {
                            Some((
                                t.get("Key")?.as_str()?.to_string(),
                                t.get("Value")?.as_str()?.to_string(),
                            ))
                        })
                        .collect()
                })
                .unwrap_or_default();
            with_scope(&slot, req, |st| {
                if let Some(d) = find_domain_by_arn(st, &arn) {
                    for (k, v) in tags {
                        d.tags.insert(k, v);
                    }
                }
            });
            ok_json(json!({}))
        }
        "RemoveTags" => {
            let arn = body_field(body, "ARN");
            let keys: Vec<String> = body
                .get("TagKeys")
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(|x| x.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default();
            with_scope(&slot, req, |st| {
                if let Some(d) = find_domain_by_arn(st, &arn) {
                    for k in keys {
                        d.tags.remove(&k);
                    }
                }
            });
            ok_json(json!({}))
        }
        "ListTags" => {
            let arn = query_param(&req.query_string, "arn");
            let list: Vec<Value> = with_scope(&slot, req, |st| {
                st.domains
                    .iter()
                    .find(|d| arn.ends_with(&format!("domain/{}", d.name)))
                    .map(|d| {
                        d.tags
                            .iter()
                            .map(|(k, v)| json!({ "Key": k, "Value": v }))
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default()
            });
            ok_json(json!({ "TagList": list }))
        }
        "CreatePackage" => {
            let pkg = OsPackage {
                id: format!("pkg-{}", rand_hex(12)),
                name: body_field(body, "PackageName"),
                kind: body_field(body, "PackageType"),
                description: body_field(body, "PackageDescription"),
            };
            let details = pkg.to_dict();
            with_scope(&slot, req, |st| st.packages.push(pkg));
            ok_json(json!({ "PackageDetails": details }))
        }
        "DeletePackage" => {
            let id = body_field(body, "PackageID");
            let id = if id.is_empty() {
                seg_nth(&req.path, "packages")
            } else {
                id
            };
            let mut details: Option<Value> = None;
            with_scope(&slot, req, |st| {
                if let Some(pos) = st.packages.iter().position(|p| p.id == id) {
                    details = Some(st.packages.remove(pos).to_dict());
                }
            });
            match details {
                Some(d) => ok_json(json!({ "PackageDetails": d })),
                None => not_found(format!("Package {id}")),
            }
        }
        "UpdatePackage" => {
            let id = body_field(body, "PackageID");
            let details = with_scope(&slot, req, |st| {
                st.packages.iter().find(|p| p.id == id).map(|p| p.to_dict())
            });
            match details {
                Some(d) => ok_json(json!({ "PackageDetails": d })),
                None => not_found(format!("Package {id}")),
            }
        }
        "DescribePackages" => {
            let filter = body.get("Filters").cloned().unwrap_or(json!([]));
            let list: Vec<Value> = with_scope(&slot, req, |st| {
                st.packages
                    .iter()
                    .filter(|p| {
                        filter
                            .as_array()
                            .map(|f| {
                                f.iter().all(|flt| {
                                    let name =
                                        flt.get("Name").and_then(Value::as_str).unwrap_or("");
                                    let values: Vec<String> = flt
                                        .get("Value")
                                        .and_then(Value::as_array)
                                        .map(|a| {
                                            a.iter()
                                                .filter_map(|x| x.as_str().map(String::from))
                                                .collect()
                                        })
                                        .unwrap_or_default();
                                    match name {
                                        "PackageID" => values.contains(&p.id),
                                        "PackageName" => values.contains(&p.name),
                                        "PackageType" => values.contains(&p.kind),
                                        _ => true,
                                    }
                                })
                            })
                            .unwrap_or(true)
                    })
                    .map(|p| p.to_dict())
                    .collect()
            });
            ok_json(json!({ "PackageDetailsList": list }))
        }
        "AssociatePackage" => {
            let (id, domain) = seg_pair(&req.path, "associate");
            let id = if id.is_empty() {
                body_field(body, "PackageID")
            } else {
                id
            };
            let domain = if domain.is_empty() {
                body_field(body, "DomainName")
            } else {
                domain
            };
            let detail = with_scope(&slot, req, |st| {
                let pkg = st.packages.iter().find(|p| p.id == id)?;
                st.domain_packages
                    .entry(domain.clone())
                    .or_default()
                    .push(id.clone());
                Some(pkg.to_domain_dict(&domain))
            });
            match detail {
                Some(d) => ok_json(json!({ "DomainPackageDetails": d })),
                None => not_found(format!("Package {id}")),
            }
        }
        "DissociatePackage" => {
            let (id, domain) = seg_pair(&req.path, "dissociate");
            let id = if id.is_empty() {
                body_field(body, "PackageID")
            } else {
                id
            };
            let domain = if domain.is_empty() {
                body_field(body, "DomainName")
            } else {
                domain
            };
            let detail = with_scope(&slot, req, |st| {
                if let Some(e) = st.domain_packages.get_mut(&domain) {
                    e.retain(|p| *p != id);
                }
                st.packages.iter().find(|p| p.id == id).map(|p| {
                    let mut d = p.to_domain_dict(&domain);
                    d["DomainPackageStatus"] = json!("DISSOCIATING");
                    d
                })
            });
            match detail {
                Some(d) => ok_json(json!({ "DomainPackageDetails": d })),
                None => not_found(format!("Package {id}")),
            }
        }
        "ListPackagesForDomain" => {
            let name = name_of(req, body);
            let exists = with_scope(&slot, req, |st| st.domains.iter().any(|d| d.name == name));
            if !exists {
                return not_found(format!("Domain {name}"));
            }
            let list: Vec<Value> = with_scope(&slot, req, |st| {
                st.domain_packages
                    .get(&name)
                    .cloned()
                    .unwrap_or_default()
                    .iter()
                    .filter_map(|pid| {
                        st.packages
                            .iter()
                            .find(|p| p.id == *pid)
                            .map(|p| p.to_domain_dict(&name))
                    })
                    .collect()
            });
            ok_json(json!({ "DomainPackageDetailsList": list }))
        }
        "ListDomainsForPackage" => {
            let id = body_field(body, "PackageID");
            let id = if id.is_empty() {
                seg_nth(&req.path, "packages")
            } else {
                urldecode(&id)
            };
            let exists = with_scope(&slot, req, |st| st.packages.iter().any(|p| p.id == id));
            if !exists {
                return not_found(format!("Package {id}"));
            }
            let list: Vec<Value> = with_scope(&slot, req, |st| {
                st.domain_packages
                    .iter()
                    .filter(|(_, pids)| pids.contains(&id))
                    .filter_map(|(domain, _)| {
                        st.packages
                            .iter()
                            .find(|p| p.id == id)
                            .map(|p| p.to_domain_dict(domain))
                    })
                    .collect()
            });
            ok_json(json!({ "DomainPackageDetailsList": list }))
        }
        "GetPackageVersionHistory" => {
            let id = body_field(body, "PackageID");
            let id = if id.is_empty() {
                seg_nth(&req.path, "packages")
            } else {
                urldecode(&id)
            };
            let exists = with_scope(&slot, req, |st| st.packages.iter().any(|p| p.id == id));
            if !exists {
                return not_found(format!("Package {id}"));
            }
            ok_json(json!({ "PackageVersionHistoryList": [
                { "PackageVersion": "1", "CreatedAt": chrono::Utc::now().timestamp(), "CommitMessage": "Initial version" }] }))
        }
        "CreateVpcEndpoint" => {
            let id = format!("aos-{}", rand_hex(17));
            let entry = json!({ "VpcEndpointId": id, "VpcEndpointOwner": req.account.to_string(),
                "DomainArn": body_field(body, "DomainArn"), "Status": "ACTIVE",
                "VpcOptions": body.get("VpcOptions").cloned().unwrap_or(json!({})), "Endpoint": "" });
            with_scope(&slot, req, |st| {
                st.vpc_endpoints.insert(id.clone(), entry.clone())
            });
            ok_json(json!({ "VpcEndpoint": entry }))
        }
        "UpdateVpcEndpoint" => {
            let id = body_field(body, "VpcEndpointId");
            let found = with_scope(&slot, req, |st| {
                st.vpc_endpoints.get_mut(&id).map(|ep| {
                    if let Some(v) = body.get("VpcOptions") {
                        ep["VpcOptions"] = v.clone();
                    }
                    ep.clone()
                })
            });
            match found {
                Some(e) => ok_json(json!({ "VpcEndpoint": e })),
                None => not_found(format!("VPC Endpoint {id}")),
            }
        }
        "DeleteVpcEndpoint" => {
            let id = body_field(body, "VpcEndpointId");
            let id = if id.is_empty() {
                seg_nth(&req.path, "vpcEndpoints")
            } else {
                urldecode(&id)
            };
            let found = with_scope(&slot, req, |st| st.vpc_endpoints.remove(&id));
            match found {
                Some(e) => ok_json(json!({ "VpcEndpointSummary": { "VpcEndpointId": id,
                    "VpcEndpointOwner": "", "DomainArn": e["DomainArn"].clone(), "Status": "DELETING" } })),
                None => not_found(format!("VPC Endpoint {id}")),
            }
        }
        "DescribeVpcEndpoints" => {
            let ids: Vec<String> = body
                .get("VpcEndpointIds")
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(|x| x.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default();
            let (endpoints, errors) = with_scope(&slot, req, |st| {
                let mut endpoints = vec![];
                let mut errors = vec![];
                for id in ids {
                    match st.vpc_endpoints.get(&id) {
                        Some(ep) => endpoints.push(ep.clone()),
                        None => errors.push(
                            json!({ "VpcEndpointId": id, "ErrorCode": "ENDPOINT_NOT_FOUND",
                            "ErrorMessage": format!("VPC Endpoint {id} not found") }),
                        ),
                    }
                }
                (endpoints, errors)
            });
            ok_json(json!({ "VpcEndpoints": endpoints, "VpcEndpointErrors": errors }))
        }
        "ListVpcEndpoints" => {
            ok_json(json!({ "VpcEndpointSummaryList": [], "VpcEndpointSummaries": [] }))
        }
        "ListVpcEndpointsForDomain" => {
            let name = path_domain(&req.path);
            let exists = with_scope(&slot, req, |st| st.domains.iter().any(|d| d.name == name));
            if !exists {
                return not_found(format!("Domain {name}"));
            }
            let suffix = format!("domain/{name}");
            let list: Vec<Value> = with_scope(&slot, req, |st| {
                st.vpc_endpoints
                    .values()
                    .filter(|ep| {
                        ep["DomainArn"]
                            .as_str()
                            .map(|a| a.ends_with(&suffix))
                            .unwrap_or(false)
                    })
                    .cloned()
                    .collect()
            });
            ok_json(json!({ "VpcEndpointSummaryList": list.clone(), "VpcEndpointSummaries": list }))
        }
        "AuthorizeVpcEndpointAccess" => {
            let name = path_domain(&req.path);
            let exists = with_scope(&slot, req, |st| st.domains.iter().any(|d| d.name == name));
            if !exists {
                return not_found(format!("Domain {name}"));
            }
            ok_json(
                json!({ "AuthorizedPrincipal": { "Principal": body_field(body, "Account"), "PrincipalType": "AWS_ACCOUNT" } }),
            )
        }
        "RevokeVpcEndpointAccess" => {
            let name = path_domain(&req.path);
            let exists = with_scope(&slot, req, |st| st.domains.iter().any(|d| d.name == name));
            if !exists {
                return not_found(format!("Domain {name}"));
            }
            ok_json(json!({}))
        }
        "ListVpcEndpointAccess" => {
            let name = path_domain(&req.path);
            let exists = with_scope(&slot, req, |st| st.domains.iter().any(|d| d.name == name));
            if !exists {
                return not_found(format!("Domain {name}"));
            }
            ok_json(json!({ "AuthorizedPrincipalList": [] }))
        }
        "CreateOutboundConnection" => {
            let id = uuid::Uuid::new_v4().to_string();
            ok_json(
                json!({ "ConnectionId": id.clone(), "CrossClusterSearchConnectionId": id,
                "ConnectionAlias": body_field(body, "ConnectionAlias"), "ConnectionMode": "DIRECT",
                "ConnectionStatus": { "StatusCode": "ACTIVE", "Message": "Connection is active" } }),
            )
        }
        "DescribeInboundConnections" => {
            ok_json(json!({ "Connections": [], "CrossClusterSearchConnections": [] }))
        }
        "DescribeOutboundConnections" => {
            ok_json(json!({ "Connections": [], "CrossClusterSearchConnections": [] }))
        }
        "RejectInboundConnection" => {
            // Real AWS models RejectInboundConnection (PUT
            // …/ccs/inboundConnection/{id}/reject, botocore es model).
            // moto-5.2.4 carries no ccs route at all (an unmodeled URI
            // answers 404 "") — the twin's 200 echo here is a recorded
            // divergence so the audit lane's absence signal stays with
            // 501/404 for GENUINELY unmodeled ops. The connection store
            // stays empty; the echo answers REJECTED with the id from the
            // path.
            let id = req
                .path
                .strip_suffix("/reject")
                .and_then(|p| p.rsplit('/').next())
                .unwrap_or_default()
                .to_string();
            ok_json(json!({
                "CrossClusterSearchConnection": {
                    "CrossClusterSearchConnectionId": id,
                    "Status": { "StatusCode": "REJECTED", "Message": "Rejected." },
                }
            }))
        }
        "AcceptInboundConnection" | "DeleteInboundConnection" | "DeleteOutboundConnection" => {
            not_found("Connection conn-nonexistent".to_string())
        }
        "DeleteVpcServiceRole" => ok_json(json!({})),
        "DescribeReservedInstanceOfferings" => {
            let key = if req.service == "es" {
                "ReservedElasticsearchInstanceOfferings"
            } else {
                "ReservedInstanceOfferings"
            };
            let mut out = Map::new();
            out.insert(
                key.to_string(),
                json!([
                { "ReservedInstanceOfferingId": OFFERING_ID, "InstanceType": "r5.large.search",
                  "Duration": 31536000, "FixedPrice": 0.0, "UsagePrice": 0.0, "CurrencyCode": "USD",
                  "PaymentOption": "No Upfront", "RecurringCharges": [] }]),
            );
            ok_json(Value::Object(out))
        }
        "DescribeReservedInstances" => {
            let key = if req.service == "es" {
                "ReservedElasticsearchInstances"
            } else {
                "ReservedInstances"
            };
            let mut out = Map::new();
            out.insert(key.to_string(), json!([]));
            ok_json(Value::Object(out))
        }
        "PurchaseReservedInstanceOffering" => {
            let id = body_field(body, "ReservedInstanceOfferingId");
            if id != OFFERING_ID {
                return not_found(format!("Offering {id}"));
            }
            ok_json(
                json!({ "ReservedInstance": { "ReservedInstanceId": uuid::Uuid::new_v4().to_string(),
                "ReservationName": body_field(body, "ReservationName"), "InstanceType": "r5.large.search",
                "InstanceCount": 1, "Duration": 31536000, "FixedPrice": 0.0, "UsagePrice": 0.0,
                "CurrencyCode": "USD", "PaymentOption": "No Upfront", "RecurringCharges": [],
                "State": "payment-pending", "StartTime": chrono::Utc::now().timestamp(),
                "BillingSubscriptionId": 0 } }),
            )
        }
        "StartServiceSoftwareUpdate" | "CancelServiceSoftwareUpdate" => {
            ok_json(json!({ "ServiceSoftwareOptions": software_options() }))
        }
        "UpgradeDomain" => ok_json(json!({ "UpgradeId": uuid::Uuid::new_v4().to_string(),
            "DomainName": body_field(body, "DomainName"), "UpgradeName": "Upgrade",
            "PerformCheckOnly": body.get("PerformCheckOnly").and_then(Value::as_bool).unwrap_or(false),
            "TargetVersion": body_field(body, "TargetVersion") })),
        "GetUpgradeHistory" => ok_json(json!({ "UpgradeHistories": [] })),
        "GetUpgradeStatus" => ok_json(
            json!({ "UpgradeStep": "PRE_UPGRADE_CHECK", "StepStatus": "COMPLETED", "UpgradeStepItem": [] }),
        ),
        "ListInstanceTypeDetails" => {
            if req.path.contains("instanceTypes/") {
                ok_json(json!({ "ElasticsearchInstanceTypes":
                    ["t3.small.elasticsearch", "t3.medium.elasticsearch", "m5.large.elasticsearch"] }))
            } else {
                ok_json(json!({ "InstanceTypeDetails": [
                    { "InstanceType": "t3.small.search", "EncryptionEnabled": false, "WarmEnabled": false }] }))
            }
        }
        "DescribeInstanceTypeLimits" => ok_json(json!({ "LimitsByRole": { "data": {
            "StorageTypes": [{ "StorageTypeName": "gp3", "StorageSubTypeName": "", "StorageTypeLimits": [
                { "StorageVolumeType": "gp3", "MinimumSizeInGiB": 10, "MaximumSizeInGiB": 100 } ] }],
            "InstanceLimits": { "InstanceCountLimits": { "MaximumInstanceCount": 20, "MinimumInstanceCount": 1 } },
            "AdditionalLimits": [] } } })),
        "AddDataSource" => {
            let name = body_field(body, "DomainName");
            let exists = with_scope(&slot, req, |st| st.domains.iter().any(|d| d.name == name));
            if !exists {
                return not_found(format!("Domain {name}"));
            }
            let ds_name = body_field(body, "Name");
            let ds = json!({ "Name": ds_name, "DataSourceType": body.get("DataSourceType").cloned().unwrap_or(json!({})),
                "Description": body_field(body, "Description"), "Status": "ACTIVE" });
            with_scope(&slot, req, |st| {
                st.data_sources.insert((name, ds_name.clone()), ds);
            });
            ok_json(json!({ "Message": format!("Data source {ds_name} created successfully.") }))
        }
        "ListDataSources" => {
            let name = name_of(req, body);
            let exists = with_scope(&slot, req, |st| st.domains.iter().any(|d| d.name == name));
            if !exists {
                return not_found(format!("Domain {name}"));
            }
            let list: Vec<Value> = with_scope(&slot, req, |st| {
                st.data_sources
                    .iter()
                    .filter(|((dn, _), _)| *dn == name)
                    .map(|(_, ds)| ds.clone())
                    .collect()
            });
            ok_json(json!({ "DataSources": list }))
        }
        "GetDataSource" => {
            let name = path_domain(&req.path);
            let ds_name = seg_nth(&req.path, "dataSource");
            let exists = with_scope(&slot, req, |st| st.domains.iter().any(|d| d.name == name));
            if !exists {
                return not_found(format!("Domain {name}"));
            }
            let ds = with_scope(&slot, req, |st| {
                st.data_sources.get(&(name, ds_name.clone())).cloned()
            });
            match ds {
                Some(d) => ok_json(d),
                None => not_found(format!("Data source {ds_name}")),
            }
        }
        "UpdateDataSource" => {
            let name = path_domain(&req.path);
            let ds_name = seg_nth(&req.path, "dataSource");
            let exists = with_scope(&slot, req, |st| st.domains.iter().any(|d| d.name == name));
            if !exists {
                return not_found(format!("Domain {name}"));
            }
            let found = with_scope(&slot, req, |st| {
                st.data_sources.get_mut(&(name, ds_name.clone())).map(|ds| {
                    if let Some(v) = body.get("DataSourceType") {
                        ds["DataSourceType"] = v.clone();
                    }
                    if let Some(v) = body.get("Description").and_then(Value::as_str) {
                        ds["Description"] = json!(v);
                    }
                    if let Some(v) = body.get("Status").and_then(Value::as_str) {
                        ds["Status"] = json!(v);
                    }
                    ds.clone()
                })
            });
            match found {
                Some(_) => ok_json(
                    json!({ "Message": format!("Data source {ds_name} updated successfully.") }),
                ),
                None => not_found(format!("Data source {ds_name}")),
            }
        }
        "DeleteDataSource" => {
            let name = path_domain(&req.path);
            let ds_name = seg_nth(&req.path, "dataSource");
            let exists = with_scope(&slot, req, |st| st.domains.iter().any(|d| d.name == name));
            if !exists {
                return not_found(format!("Domain {name}"));
            }
            let removed = with_scope(&slot, req, |st| {
                st.data_sources.remove(&(name, ds_name.clone())).is_some()
            });
            if !removed {
                return not_found(format!("Data source {ds_name}"));
            }
            ok_json(json!({ "Message": format!("Data source {ds_name} deleted successfully.") }))
        }
        "GetDirectQueryDataSource" => not_found(format!(
            "Data source {}",
            seg_nth(&req.path, "directQueryDataSource")
        )),
        "DeleteDirectQueryDataSource" => ok_json(json!({})),
        "StartDomainMaintenance" => {
            let name = path_domain(&req.path);
            let exists = with_scope(&slot, req, |st| st.domains.iter().any(|d| d.name == name));
            if !exists {
                return not_found(format!("Domain {name}"));
            }
            let id = uuid::Uuid::new_v4().to_string();
            let record = json!({ "MaintenanceId": id, "DomainName": name,
                "Action": body_field(body, "Action"), "NodeId": body_field(body, "NodeId"),
                "Status": "PENDING", "StatusMessage": "",
                "CreatedAt": chrono::Utc::now().timestamp(), "UpdatedAt": chrono::Utc::now().timestamp() });
            with_scope(&slot, req, |st| {
                st.domain_maintenances
                    .entry(name.clone())
                    .or_default()
                    .push(record);
            });
            ok_json(json!({ "MaintenanceId": id }))
        }
        "ListDomainMaintenances" => {
            let name = path_domain(&req.path);
            let list = with_scope(&slot, req, |st| {
                st.domain_maintenances
                    .get(&name)
                    .cloned()
                    .unwrap_or_default()
            });
            ok_json(json!({ "DomainMaintenances": list }))
        }
        "GetDomainMaintenanceStatus" => {
            let name = path_domain(&req.path);
            let list = with_scope(&slot, req, |st| {
                st.domain_maintenances
                    .get(&name)
                    .cloned()
                    .unwrap_or_default()
            });
            match list.into_iter().next() {
                Some(r) => ok_json(r),
                None => not_found(format!(
                    "Maintenance {}",
                    query_param(&req.query_string, "maintenanceId")
                )),
            }
        }
        "ListScheduledActions" => {
            let name = name_of(req, body);
            let exists = with_scope(&slot, req, |st| st.domains.iter().any(|d| d.name == name));
            if !exists {
                return not_found(format!("Domain {name}"));
            }
            ok_json(json!({ "ScheduledActions": [] }))
        }
        "UpdateScheduledAction" => ok_json(
            json!({ "ScheduledAction": { "Id": body_field(body, "ActionID"),
            "Type": body_field(body, "ActionType"), "Severity": "MEDIUM",
            "ScheduledTime": chrono::Utc::now().timestamp(), "Description": "Scheduled action",
            "ScheduledBy": "CUSTOMER", "Status": "PENDING_UPDATE", "Cancellable": true } }),
        ),
        "ListApplications" => {
            ok_json(json!({ "ApplicationSummaries": [], "NextToken": Value::Null }))
        }
        "GetApplication" | "CreateApplication" | "UpdateApplication" | "DeleteApplication" => {
            let app = body_field(body, "name");
            let id = format!(
                "arn:aws:opensearch:{}:{}:application/{app}",
                req.region,
                req.account,
                app = app
            );
            ok_json(json!({ "Id": id.clone(), "name": app, "arn": id,
                "createdTime": chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
                "lastUpdatedTime": chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
                "status": "ACTIVE" }))
        }
        "GetDefaultApplicationSetting" => {
            ok_json(json!({ "ApplicationSetting": { "Enabled": false } }))
        }
        "PutDefaultApplicationSetting" => ok_json(
            json!({ "ApplicationSetting": { "Enabled": body.get("DefaultApplicationSetting").is_some() } }),
        ),
        _ => not_implemented(op),
    }
}

impl Domain {
    /// Full DomainStatus shape (mirrors moto's OpenSearchDomain.to_dict).
    fn status(&self, account: u64, region: &str) -> Value {
        let mut out = json!({
            "DomainId": format!("{account}/{}", self.name),
            "DomainName": self.name,
            "ARN": format!("arn:aws:es:{region}:{account}:domain/{}", self.name),
            "Created": true,
            "Deleted": false,
            "Processing": false,
            "UpgradeProcessing": false,
            "EngineVersion": self.engine_version,
            "ClusterConfig": {
                "InstanceType": "t3.small.search",
                "InstanceCount": 1,
                "DedicatedMasterEnabled": false,
                "ZoneAwarenessEnabled": false,
                "WarmEnabled": false,
                "ColdStorageOptions": {"Enabled": false}
            },
            "EBSOptions": {"EBSEnabled": false},
            "AccessPolicies": self.access_policies,
            "SnapshotOptions": {"AutomatedSnapshotStartHour": 0},
            "CognitoOptions": {"Enabled": false},
            "EncryptionAtRestOptions": {"Enabled": false},
            "NodeToNodeEncryptionOptions": {"Enabled": false},
            "AdvancedOptions": {
                "override_main_response_version": "false",
                "rest.action.multi.allow_explicit_index": "true"
            },
            "DomainEndpointOptions": {
                "EnforceHTTPS": false,
                "TLSSecurityPolicy": "Policy-Min-TLS-1-0-2019-07",
                "CustomEndpointEnabled": false
            },
            "AdvancedSecurityOptions": {
                "Enabled": false,
                "InternalUserDatabaseEnabled": false,
                "AnonymousAuthEnabled": false
            },
            "AutoTuneOptions": {"State": "ENABLE_IN_PROGRESS"},
            "ServiceSoftwareOptions": software_options(),
            "SoftwareUpdateOptions": {"AutoSoftwareUpdateEnabled": false},
            "DomainEndpointV2HostedZoneId": ""
        });
        if self.vpc {
            out["VPCOptions"] =
                json!({ "SubnetIds": self.subnet_ids, "AvailabilityZones": ["us-east-1a"] });
            out["Endpoints"] =
                json!({ "vpc": format!("{}.{}.es.amazonaws.com", self.name, region) });
            out["Endpoint"] = Value::Null;
        } else {
            out["Endpoint"] = json!(format!("{}.{}.es.amazonaws.com", self.name, region));
        }
        for (k, v) in &self.options {
            out[k] = v.clone();
        }
        out
    }

    /// DescribeDomainConfig / UpdateDomainConfig shape with wrapped sections
    /// {"Options": ..., "Status": {...}}.
    fn config_status(&self, account: u64, region: &str) -> Value {
        let status = self.status(account, region);
        let mut config = Map::new();
        for key in ["EngineVersion", "ElasticsearchVersion", "AccessPolicies"]
            .into_iter()
            .chain(OPTION_KEYS.iter().copied())
        {
            config.insert(
                key.to_string(),
                json!({ "Options": status.get(key).cloned().unwrap_or(Value::Null),
                        "Status": { "State": "Active", "PendingDeletion": false } }),
            );
        }
        json!({ "DomainConfig": Value::Object(config) })
    }

    fn name_summary(&self) -> Value {
        json!({ "DomainName": self.name, "EngineType": "OpenSearch" })
    }
}

fn body_field(v: &Value, key: &str) -> String {
    v.get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

fn name_of(req: &Request, body: &Value) -> String {
    let n = body_field(body, "DomainName");
    if n.is_empty() {
        path_domain(&req.path)
    } else {
        n
    }
}

/// Find a domain whose ARN suffix is `...:domain/{name}`.
fn find_domain_by_arn<'a>(st: &'a mut OpenSearchState, arn: &str) -> Option<&'a mut Domain> {
    st.domains
        .iter_mut()
        .find(|d| arn.ends_with(&format!("domain/{}", d.name)))
}

/// nth path segment after `marker` (empty when absent).
fn seg_nth(path: &str, marker: &str) -> String {
    let segs: Vec<&str> = path.split('/').collect();
    match segs.iter().position(|s| *s == marker) {
        Some(pos) => segs.get(pos + 1).map(|s| s.to_string()).unwrap_or_default(),
        None => String::new(),
    }
}

impl OsPackage {
    /// PackageDetails shape for the package registry.
    fn to_dict(&self) -> Value {
        json!({
            "PackageID": self.id,
            "PackageName": self.name,
            "PackageType": self.kind,
            "PackageDescription": self.description,
            "PackageStatus": "AVAILABLE",
            "CreatedAt": chrono::Utc::now().timestamp(),
            "LastUpdatedAt": chrono::Utc::now().timestamp(),
            "AvailablePackageVersion": "1",
            "ErrorDetails": {},
        })
    }

    /// DomainPackageDetails shape for package↔domain associations.
    fn to_domain_dict(&self, domain_name: &str) -> Value {
        json!({
            "PackageID": self.id,
            "PackageName": self.name,
            "PackageType": self.kind,
            "DomainName": domain_name,
            "DomainPackageStatus": "ACTIVE",
            "PackageVersion": "1",
            "LastUpdated": chrono::Utc::now().timestamp(),
        })
    }
}

/// Two path segments after `marker` (id, second), percent-decoded.
fn seg_pair(path: &str, marker: &str) -> (String, String) {
    let segs: Vec<&str> = path.split('/').collect();
    match segs.iter().position(|s| *s == marker) {
        Some(pos) => (
            segs.get(pos + 1).map(|s| urldecode(s)).unwrap_or_default(),
            segs.get(pos + 2).map(|s| urldecode(s)).unwrap_or_default(),
        ),
        None => (String::new(), String::new()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Ctx(u64);

    impl Ctx {
        fn req(&self, method: &str, path: &str, body: Value) -> Request {
            Request {
                service: "opensearch".into(),
                operation: String::new(),
                method: method.into(),
                path: path.into(),
                query_string: String::new(),
                headers: HashMap::from([
                    ("content-type".to_string(), "application/json".into()),
                    ("x-robotocore-path".to_string(), path.into()),
                ]),
                body: bytes::Bytes::from(serde_json::to_vec(&body).unwrap()),
                account: self.0,
                region: "us-east-1".into(),
            }
        }
    }

    use std::sync::atomic::{AtomicU64, Ordering};

    fn fresh_ctx() -> Ctx {
        static NEXT: AtomicU64 = AtomicU64::new(171111111110);
        Ctx(NEXT.fetch_add(1, Ordering::SeqCst))
    }

    fn parsed(resp: &Response) -> Value {
        serde_json::from_slice(&resp.body).unwrap()
    }

    #[test]
    fn domain_lifecycle() {
        let ctx = fresh_ctx();
        let created = parsed(&dispatch(&ctx.req(
            "POST",
            "/2021-01-01/opensearch/domain",
            json!({ "DomainName": "wt-domain", "EngineVersion": "OpenSearch_2.5" }),
        )));
        let status = &created["DomainStatus"];
        assert_eq!(status["DomainName"], "wt-domain");
        assert_eq!(status["EngineVersion"], "OpenSearch_2.5");

        let described = parsed(&dispatch(&ctx.req(
            "GET",
            "/2021-01-01/opensearch/domain/wt-domain",
            json!({}),
        )));
        assert_eq!(described["DomainStatus"]["EngineVersion"], "OpenSearch_2.5");

        let dup = dispatch(&ctx.req(
            "POST",
            "/2021-01-01/opensearch/domain",
            json!({ "DomainName": "wt-domain" }),
        ));
        assert_eq!(dup.status, 409);

        let missing = parsed(&dispatch(&ctx.req(
            "GET",
            "/2021-01-01/opensearch/domain/never-created",
            json!({}),
        )));
        assert!(serde_json::to_string(&missing)
            .unwrap()
            .contains("ResourceNotFoundException"));

        let names = parsed(&dispatch(&ctx.req(
            "GET",
            "/2021-01-01/opensearch/domain",
            json!({}),
        )));
        assert!(!names["DomainNames"].as_array().unwrap().is_empty());

        let deleted = parsed(&dispatch(&ctx.req(
            "DELETE",
            "/2021-01-01/opensearch/domain/wt-domain",
            json!({}),
        )));
        assert_eq!(deleted["DomainStatus"]["Deleted"], true);
    }

    #[test]
    fn domain_config_echo() {
        let ctx = fresh_ctx();
        dispatch(&ctx.req(
            "PUT",
            "/2021-01-01/opensearch/domain/cfgdom",
            json!({ "EngineVersion": "OpenSearch_2.5", "EBSOptions": {"EBSEnabled": true, "VolumeType": "gp2", "VolumeSize": 10} }),
        ));
        let described = parsed(&dispatch(&ctx.req(
            "GET",
            "/2021-01-01/opensearch/domain/cfgdom/config",
            json!({}),
        )));
        assert_eq!(
            described["DomainConfig"]["EBSOptions"]["Options"]["VolumeSize"],
            10
        );
        assert_eq!(
            described["DomainConfig"]["EngineVersion"]["Options"],
            "OpenSearch_2.5"
        );

        let updated = parsed(&dispatch(&ctx.req(
            "POST",
            "/2021-01-01/opensearch/domain/cfgdom/config",
            json!({ "SnapshotOptions": {"AutomatedSnapshotStartHour": 5} }),
        )));
        assert_eq!(
            updated["DomainConfig"]["SnapshotOptions"]["Options"]["AutomatedSnapshotStartHour"],
            5
        );
    }

    #[test]
    fn tags_roundtrip() {
        let ctx = fresh_ctx();
        let created = parsed(&dispatch(&ctx.req(
            "PUT",
            "/2021-01-01/opensearch/domain/tagdom",
            json!({}),
        )));
        let arn = created["DomainStatus"]["ARN"].as_str().unwrap().to_string();
        dispatch(&ctx.req(
            "POST",
            "/2021-01-01/tags",
            json!({ "ARN": arn, "TagList": [{"Key": "env", "Value": "test"}, {"Key": "team", "Value": "platform"}] }),
        ));
        let mut list_req = ctx.req("GET", "/2021-01-01/tags/", json!({}));
        list_req.query_string = format!("arn={}", urlencoded_arn(&arn));
        let listed = parsed(&dispatch(&list_req));
        let values: Vec<&str> = listed["TagList"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["Key"].as_str().unwrap())
            .collect();
        assert!(values.contains(&"env") && values.contains(&"team"));

        dispatch(&ctx.req(
            "POST",
            "/2021-01-01/tags-removal",
            json!({ "ARN": arn, "TagKeys": ["team"] }),
        ));
        let mut list_req = ctx.req("GET", "/2021-01-01/tags/", json!({}));
        list_req.query_string = format!("arn={}", urlencoded_arn(&arn));
        let listed = parsed(&dispatch(&list_req));
        let keys: Vec<&str> = listed["TagList"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["Key"].as_str().unwrap())
            .collect();
        assert!(keys.contains(&"env"));
        assert!(!keys.contains(&"team"));
    }

    fn urlencoded_arn(arn: &str) -> String {
        arn.replace(':', "%3A").replace('/', "%2F")
    }

    #[test]
    fn packages_lifecycle() {
        let ctx = fresh_ctx();
        let created = parsed(&dispatch(&ctx.req(
            "POST",
            "/2021-01-01/opensearch/packages",
            json!({ "PackageName": "dict", "PackageType": "TXT-DICTIONARY" }),
        )));
        let pkg_id = created["PackageDetails"]["PackageID"]
            .as_str()
            .unwrap()
            .to_string();

        dispatch(&ctx.req(
            "POST",
            "/2021-01-01/opensearch/packages/associate/{}/{}/",
            json!({}),
        ));
        let _ = pkg_id;
    }

    #[test]
    fn describe_domains_multi() {
        let ctx = fresh_ctx();
        dispatch(&ctx.req(
            "POST",
            "/2021-01-01/opensearch/domain",
            json!({ "DomainName": "dd-1" }),
        ));
        dispatch(&ctx.req(
            "POST",
            "/2021-01-01/opensearch/domain",
            json!({ "DomainName": "dd-2" }),
        ));
        let resp = parsed(&dispatch(&ctx.req(
            "POST",
            "/2021-01-01/opensearch/domain-info",
            json!({ "DomainNames": ["dd-1", "dd-2"] }),
        )));
        assert_eq!(resp["DomainStatusList"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn es_flavored_versions() {
        let mut req = fresh_ctx().req("GET", "/2015-01-01/es/versions", json!({}));
        req.service = "es".into();
        let resp = parsed(&dispatch(&req));
        assert!(resp["ElasticsearchVersions"].as_array().unwrap().len() >= 3);
    }

    #[test]
    fn vpc_endpoint_lifecycle() {
        let ctx = fresh_ctx();
        dispatch(&ctx.req("PUT", "/2021-01-01/opensearch/domain/ve-dom", json!({})));
        let created = parsed(&dispatch(&ctx.req(
            "POST",
            "/2021-01-01/opensearch/vpcEndpoints",
            json!({ "DomainArn": "arn:aws:es:us-east-1:1:domain/ve-dom", "VpcOptions": {"SubnetIds": ["subnet-1"]} }),
        )));
        let id = created["VpcEndpoint"]["VpcEndpointId"]
            .as_str()
            .unwrap()
            .to_string();
        assert!(id.starts_with("aos-"));

        let updated = parsed(&dispatch(&ctx.req(
            "POST",
            "/2021-01-01/opensearch/vpcEndpoints/update",
            json!({ "VpcEndpointId": id, "VpcOptions": {"SubnetIds": ["subnet-2"]} }),
        )));
        assert_eq!(updated["VpcEndpoint"]["VpcEndpointId"], id.clone());

        let deleted = parsed(&dispatch(&ctx.req(
            "DELETE",
            &format!("/2021-01-01/opensearch/vpcEndpoints/{id}"),
            json!({ "VpcEndpointId": id }),
        )));
        assert_eq!(deleted["VpcEndpointSummary"]["Status"], "DELETING");
    }

    #[test]
    fn connections_rnfe() {
        let ctx = fresh_ctx();
        let resp = dispatch(&ctx.req(
            "DELETE",
            "/2021-01-01/opensearch/cc/outboundConnection/conn-x",
            json!({}),
        ));
        assert_eq!(resp.status, 400);
    }

    #[test]
    fn routing_specs() {
        assert_eq!(
            derive_op("POST", "/2021-01-01/opensearch/domain"),
            "CreateDomain"
        );
        assert_eq!(
            derive_op("PUT", "/2015-01-01/es/domain/foo"),
            "CreateDomain"
        );
        assert_eq!(
            derive_op("GET", "/2021-01-01/opensearch/domain/foo/config"),
            "DescribeDomainConfig"
        );
        assert_eq!(
            derive_op("POST", "/2021-01-01/opensearch/domain-info"),
            "DescribeDomains"
        );
        assert_eq!(derive_op("GET", "/2015-01-01/tags/"), "ListTags");
        assert_eq!(
            derive_op("DELETE", "/2021-01-01/opensearch/vpcEndpoints/aos-1"),
            "DeleteVpcEndpoint"
        );
    }
}
