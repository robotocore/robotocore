//! AWS Panorama backend — port of moto's `panorama` (rest-json, one URI per
//! operation).
//!
//! Requests enter with the panorama sigv4 scope and an empty `operation`, so
//! this module resolves the operation from the HTTP method + botocore
//! requestUri, like the `ram`/`mq` ports. State is per-(account, region);
//! moto holds devices, packages, nodes (MEDIA_SOURCE packages created by
//! RTSP template jobs), template jobs, package-import jobs, application
//! instances, device jobs, registered package versions and an ARN-keyed tag
//! store overlaid on top of resources' built-in tags.
//!
//! Known moto surface quirks kept faithful (fork `moto/panorama`):
//! - `UpdateDeviceMetadata` / `DeleteDevice` on an unknown device raise a
//!   bare `KeyError` moto-side → HTTP 500 `InternalError` with the
//!   repr-quoted id (as recorded in probes/panorama.json).
//! - `ListNodes` / `ListNodeFromTemplateJobs` / `ListPackages` /
//!   `ListPackageImportJobs` read `category`/`maxResults`/`nextToken`
//!   lowercase while botocore sends capitalised query keys, so the filters
//!   never fire; `ListNodes` therefore always returns an empty list (the
//!   `x.category == None` filter matches nothing).
//! - `ListApplicationInstances` and the dependency / node-instance lists
//!   read their list params lowercase too: unfiltered and unpaginated.
//! - `DescribePackageVersion` reads `ownerAccount` (never sent) so the owner
//!   account is always the caller's.
//! - A template job of type `RTSP_CAMERA_STREAM` silently creates a
//!   MEDIA_SOURCE package in the nodes store (visible to `DescribeNode`).
//! - `NodeDescription` is stored from `node_name` (moto's argument slip).
//! - Device managed statuses use moto's default "immediate" progression:
//!   `DeviceAggregatedStatus` is always `ONLINE`, `ProvisioningStatus` and
//!   the provision response `Status` always `SUCCEEDED`.

use crate::proto::{json_error, parse_json_body, query_map, url_decode};
use crate::store::{self, ScopeKey};
use crate::{Request, Response};
use base64::engine::general_purpose::{STANDARD as B64, URL_SAFE_NO_PAD};
use base64::Engine as _;
use chrono::{DateTime, Duration, Utc};
use parking_lot::RwLock;
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

type Stores = Arc<RwLock<HashMap<ScopeKey, PanoramaStore>>>;

fn stores() -> Stores {
    store::slot::<PanoramaStore>("panorama")
}

/// Per-(account, region) in-memory state for AWS Panorama. Vec-backed
/// collections keep moto's insertion order for the list operations; removals
/// find by id exactly like moto's dict pops.
#[derive(Default)]
struct PanoramaStore {
    devices: Vec<Device>,
    /// moto's `node_from_template_memory`.
    node_jobs: Vec<Node>,
    /// moto's `nodes_memory` — only MEDIA_SOURCE packages from RTSP jobs.
    nodes: Vec<Package>,
    /// moto's `packages_memory` — BUSINESS_LOGIC packages.
    packages: Vec<Package>,
    package_import_jobs: Vec<PackageImportJob>,
    application_instances: Vec<ApplicationInstance>,
    device_jobs: Vec<DeviceJob>,
    /// moto's `package_versions_memory`, keyed `{package_id}/{version}/{patch}`.
    package_versions: BTreeMap<String, PackageVersion>,
    /// moto's `_tags` store — arn -> tag map.
    tags: BTreeMap<String, BTreeMap<String, String>>,
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "panorama" {
        return None;
    }
    let body = parse_json_body(&req.body);
    let query = query_map(&req.query_string);
    let slot = stores();
    let mut map = slot.write();
    let st = map.entry((req.account, req.region.clone())).or_default();
    Some(route(st, req, &body, &query))
}

fn not_implemented(op: &str) -> Response {
    let e = json!({
        "__type": "NotImplemented",
        "message": format!("The {op} action has not been implemented"),
    });
    Response::json(501, serde_json::to_vec(&e).unwrap_or_default())
}

fn route(
    st: &mut PanoramaStore,
    req: &Request,
    body: &Value,
    query: &BTreeMap<String, Vec<String>>,
) -> Response {
    let account = req.account;
    let region = req.region.clone();
    let segs: Vec<&str> = req
        .path
        .trim_start_matches('/')
        .trim_end_matches('/')
        .split('/')
        .collect();
    match (req.method.as_str(), segs.as_slice()) {
        ("POST", ["devices"]) => provision_device(st, &region, account, body),
        ("GET", ["devices"]) => list_devices(st, query),
        ("GET", ["devices", id]) => describe_device(st, id),
        ("PUT", ["devices", id]) => update_device_metadata(st, id, body),
        ("DELETE", ["devices", id]) => delete_device(st, id),
        ("GET", ["nodes"]) => list_nodes(),
        ("GET", ["nodes", id]) => describe_node(st, &region, account, id),
        ("POST", ["application-instances"]) => {
            create_application_instance(st, &region, account, body)
        }
        ("GET", ["application-instances"]) => list_application_instances(st),
        ("GET", ["application-instances", id, "details"]) => describe_application_instance(st, id),
        ("GET", ["application-instances", id, "package-dependencies"]) => list_dependencies(st, id),
        ("GET", ["application-instances", id, "node-instances"]) => list_node_instances(st, id),
        ("PUT", ["application-instances", id, "node-signals"]) => signal_node_instances(st, id),
        ("GET", ["application-instances", id]) => describe_application_instance(st, id),
        ("DELETE", ["application-instances", id]) => {
            remove_application_instance(st, &region, account, id)
        }
        ("POST", ["jobs"]) => create_job_for_devices(st, &region, account, body),
        ("GET", ["jobs"]) => list_devices_jobs(st, query),
        ("GET", ["jobs", id]) => describe_device_job(st, id),
        ("POST", ["packages"]) => create_package(st, &region, account, body),
        ("GET", ["packages"]) => list_packages(st, &region, account),
        ("DELETE", ["packages", id]) => delete_package(st, id),
        ("GET", ["packages", "template-job"]) => list_template_jobs(st),
        ("POST", ["packages", "template-job"]) => {
            create_node_from_template_job(st, &region, account, body)
        }
        ("GET", ["packages", "template-job", id]) => describe_node_from_template_job(st, id),
        ("GET", ["packages", "metadata", id]) => describe_package(st, &region, account, id),
        ("GET", ["packages", "metadata", id, "versions", version]) => describe_package_version(
            st,
            &region,
            account,
            id,
            version,
            query_str(query, "PatchVersion"),
        ),
        ("GET", ["packages", "import-jobs"]) => list_package_import_jobs(st),
        ("POST", ["packages", "import-jobs"]) => {
            create_package_import_job(st, &region, account, body)
        }
        ("GET", ["packages", "import-jobs", id]) => describe_package_import_job(st, id),
        ("PUT", ["packages", id, "versions", version, "patch", patch]) => {
            register_package_version(st, &region, account, id, version, patch, body)
        }
        ("DELETE", ["packages", id, "versions", version, "patch", patch]) => {
            deregister_package_version(st, id, version, patch)
        }
        ("GET", ["tags", ..]) => list_tags_for_resource(st, &tags_arn(req)),
        ("POST", ["tags", ..]) => tag_resource(st, &tags_arn(req), body),
        ("DELETE", ["tags", ..]) => untag_resource(st, &tags_arn(req), query),
        _ => not_implemented(&req.operation),
    }
}

/// The resource ARN after `/tags/` — botocore percent-encodes the ARN (its
/// slashes collapse into `%2F`), moto's regex also accepts stray slashes.
fn tags_arn(req: &Request) -> String {
    let path = req.path.trim_start_matches('/');
    match path.split_once('/') {
        Some((_, rest)) => url_decode(rest.trim_end_matches('/')),
        None => String::new(),
    }
}

/// Query-param readers for the two list ops whose pagination the fork really
/// parses (capitalised, as botocore sends them): MaxResults/NextToken.
fn list_params(query: &BTreeMap<String, Vec<String>>) -> (Option<i64>, Option<String>) {
    let max = query
        .get("MaxResults")
        .and_then(|v| v.first())
        .and_then(|v| v.parse::<i64>().ok());
    let token = query
        .get("NextToken")
        .and_then(|v| v.first())
        .map(String::from);
    (max, token)
}

fn query_str(query: &BTreeMap<String, Vec<String>>, key: &str) -> Option<String> {
    query.get(key).and_then(|v| v.first()).map(String::from)
}

fn body_str(body: &Value, key: &str) -> Option<String> {
    body.get(key).and_then(Value::as_str).map(String::from)
}

// ---------------------------------------------------------------------------
// Response helpers
// ---------------------------------------------------------------------------

fn ok_json(v: Value) -> Response {
    Response::json(200, serde_json::to_vec(&v).unwrap_or_default())
}

fn err(status: u16, code: &str, message: String) -> Response {
    json_error(status, code, &message)
}

/// moto's `ValidationError` (RESTError default 400).
fn validation(message: String) -> Response {
    err(400, "ValidationException", message)
}

/// moto's `ResourceNotFoundException` (code 404).
fn not_found(message: String) -> Response {
    err(404, "ResourceNotFoundException", message)
}

/// moto's bare `KeyError` on unknown devices: HTTP 500 `InternalError` with
/// the repr-quoted key as message (probe-recorded behavior).
fn internal_error(key: &str) -> Response {
    err(500, "InternalError", format!("'{key}'"))
}

fn list_response(key: &str, items: Vec<Value>, next_token: Option<String>) -> Response {
    ok_json(json!({ key: items, "NextToken": next_token }))
}

// ---------------------------------------------------------------------------
// Shared helpers (mirroring moto/panorama/utils.py)
// ---------------------------------------------------------------------------

/// moto's `arn_formatter`.
fn arn_for(kind: &str, id: &str, account: u64, region: &str) -> String {
    format!(
        "{}:panorama:{region}:{account}:{kind}/{id}",
        partition(region)
    )
}

/// moto's `get_partition` over the regions Panorama serves.
fn partition(region: &str) -> &'static str {
    match region {
        "us-gov-west-1" | "us-gov-east-1" => "arn:aws-us-gov",
        "cn-north-1" | "cn-northwest-1" => "arn:aws-cn",
        _ => "arn:aws",
    }
}

/// moto's `hash_name`: urlsafe-unpadded base64 of the md5 digest.
fn hash_name(name: &str) -> String {
    URL_SAFE_NO_PAD.encode(md5::compute(name.as_bytes()).0)
}

/// moto's `generate_package_id`: alphanumeric md5 token repeated to 64 chars.
fn generate_package_id(name: &str) -> String {
    let base = B64
        .encode(md5::compute(name.as_bytes()).0)
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .collect::<String>();
    let mut token = base.clone();
    while token.len() < 64 {
        token.push_str(&base);
    }
    token.truncate(64);
    token.to_ascii_lowercase()
}

/// Python `datetime.isoformat()` — what moto's
/// `deep_convert_datetime_to_isoformat` puts on the wire.
fn iso(dt: DateTime<Utc>) -> String {
    dt.format("%Y-%m-%dT%H:%M:%S%.6f+00:00").to_string()
}

/// Python `str(datetime)` (space separator) — feeds the application-instance
/// id hash input only.
fn dt_space(dt: DateTime<Utc>) -> String {
    dt.format("%Y-%m-%d %H:%M:%S%.6f+00:00").to_string()
}

/// moto's `Package.storage_location`.
fn storage_location(account: u64, region: &str) -> Value {
    let prefix = format!("s3://panorama-{region}/{account}");
    json!({
        "BinaryPrefixLocation": format!("{prefix}/binaries/"),
        "Bucket": format!("panorama-{region}"),
        "GeneratedPrefixLocation": format!("{prefix}/generated/"),
        "ManifestPrefixLocation": format!("{prefix}/manifests/"),
        "RepoPrefixLocation": format!("{prefix}/repo/"),
    })
}

/// Parse an incoming Tags object into moto's stored shape.
fn tags_from(v: Option<&Value>) -> Option<BTreeMap<String, String>> {
    let obj = v?.as_object()?;
    let mut out = BTreeMap::new();
    for (k, val) in obj {
        if let Some(s) = val.as_str() {
            out.insert(k.clone(), s.to_string());
        }
    }
    Some(out)
}

fn tags_json(tags: &BTreeMap<String, String>) -> Value {
    let mut map = Map::new();
    for (k, v) in tags {
        map.insert(k.clone(), json!(v));
    }
    Value::Object(map)
}

/// moto: `base64.b64encode(b"certificate")` for the provision response.
fn base64_certificate() -> String {
    B64.encode(b"certificate")
}

/// moto's `Paginator.paginate` over `items`, keyed by each item's
/// `unique_key`. Returns `Err` for invalid or stale tokens (moto's
/// `InvalidToken` 400s).
fn paginate(
    items: Vec<Value>,
    max_results: Option<i64>,
    next_token: Option<String>,
    default_limit: i64,
    unique_key: &str,
) -> Result<(Vec<Value>, Option<String>), Response> {
    let limit = match max_results {
        Some(n) if n > 0 => n as usize,
        _ => default_limit.max(0) as usize,
    };
    let start = match next_token.as_deref() {
        None | Some("") => 0,
        Some(token) => {
            let decoded = B64
                .decode(token)
                .ok()
                .and_then(|raw| String::from_utf8(raw).ok())
                .and_then(|text| serde_json::from_str::<Value>(&text).ok())
                .filter(|v| v.get("uniqueAttributes").and_then(Value::as_str).is_some());
            let want = decoded
                .as_ref()
                .and_then(|v| v["uniqueAttributes"].as_str())
                .unwrap_or_default()
                .to_string();
            match items.iter().position(|v| v[unique_key] == json!(want)) {
                Some(i) if decoded.is_some() => i,
                _ => {
                    return Err(err(
                        400,
                        "InvalidToken",
                        "Invalid Token: Invalid token".to_string(),
                    ))
                }
            }
        }
    };
    if start > items.len() {
        return Err(err(
            400,
            "InvalidToken",
            "Invalid Token: Resource not found!".to_string(),
        ));
    }
    let end = (start + limit).min(items.len());
    let page = items[start..end].to_vec();
    let token = if !page.is_empty() && end < items.len() {
        // moto derives the next token from `results[index_end]` — the first
        // item of the *next* page — naming it "last on this page".
        let attr = items[end][unique_key]
            .as_str()
            .unwrap_or_default()
            .to_string();
        Some(B64.encode(
            serde_json::to_string(&json!({ "uniqueAttributes": attr })).unwrap_or_default(),
        ))
    } else {
        None
    };
    Ok((page, token))
}

// ---------------------------------------------------------------------------
// Records
// ---------------------------------------------------------------------------

struct Device {
    device_id: String,
    name: String,
    arn: String,
    description: Option<String>,
    tags: Option<BTreeMap<String, String>>,
    network_configuration: Option<Value>,
    created: DateTime<Utc>,
    last_updated: DateTime<Utc>,
    /// Captured at creation — moto's `current_networking_status` blob.
    networking_status: Value,
}

impl Device {
    fn new(account: u64, region: &str, name: &str) -> Self {
        let now = Utc::now();
        Device {
            device_id: format!("device-{}", hash_name(name)),
            name: name.to_string(),
            arn: arn_for("device", name, account, region),
            description: None,
            tags: None,
            network_configuration: None,
            created: now,
            last_updated: now,
            networking_status: json!({
                "Ethernet0Status": {
                    "ConnectionStatus": "CONNECTED",
                    "HwAddress": "8C:0F:5F:60:F5:C4",
                    "IpAddress": "192.168.1.300/24",
                },
                "Ethernet1Status": {
                    "ConnectionStatus": "NOT_CONNECTED",
                    "HwAddress": "8C:0F:6F:60:F4:F1",
                    "IpAddress": "--",
                },
                "LastUpdatedTime": iso(now),
                "NtpStatus": {
                    "ConnectionStatus": "CONNECTED",
                    "IpAddress": "91.224.149.41:123",
                    "NtpServerName": "0.pool.ntp.org",
                },
            }),
        }
    }

    /// moto's `Device.response_listed()`.
    fn listed(&self) -> Value {
        let mut out = json!({
            "Brand": "AWS_PANORAMA",
            "CreatedTime": iso(self.created),
            "CurrentSoftware": "6.2.1",
            "DeviceAggregatedStatus": "ONLINE",
            "DeviceId": self.device_id,
            "LastUpdatedTime": iso(self.last_updated),
            "LatestDeviceJob": {"JobType": "REBOOT", "Status": "COMPLETED"},
            "LeaseExpirationTime": iso(self.created + Duration::days(5)),
            "Name": self.name,
            "ProvisioningStatus": "SUCCEEDED",
            "Type": "PANORAMA_APPLIANCE",
        });
        if let Some(d) = &self.description {
            out["Description"] = json!(d);
        }
        if let Some(t) = &self.tags {
            out["Tags"] = tags_json(t);
        }
        out
    }

    /// moto's `Device.response_object()` (describe) on top of the listed shape.
    fn described(&self) -> Value {
        let mut out = self.listed();
        out["AlternateSoftwares"] = json!([{"Version": "0.2.1"}]);
        out["Arn"] = json!(self.arn);
        out["CurrentNetworkingStatus"] = self.networking_status.clone();
        out["DeviceConnectionStatus"] = json!("ONLINE");
        out["LatestSoftware"] = json!("6.2.1");
        out["SerialNumber"] = json!("GAD81E29013274749");
        out
    }
}

struct Package {
    category: String,
    description: Option<String>,
    name: String,
    created: DateTime<Utc>,
    last_updated: DateTime<Utc>,
    package_name: String,
    patch_version: String,
    package_version: String,
    output_package_name: String,
    owner_account: String,
    package_id: String,
    package_arn: String,
    tags: Option<BTreeMap<String, String>>,
}

impl Package {
    /// moto's `Package` for `CreatePackage` (BUSINESS_LOGIC) and the RTSP
    /// template job's MEDIA_SOURCE node package.
    fn new(
        account: u64,
        region: &str,
        name: &str,
        package_name: &str,
        package_version: &str,
        description: Option<String>,
        category: &str,
    ) -> Self {
        let patch_version = generate_package_id(package_name);
        let package_id = format!("package-{}", hash_name(package_name));
        let now = Utc::now();
        Package {
            category: category.to_string(),
            description,
            name: name.to_string(),
            created: now,
            last_updated: now,
            package_name: package_name.to_string(),
            output_package_name: format!(
                "{package_name}-{package_version}-{}-{name}",
                &patch_version[..8]
            ),
            package_version: package_version.to_string(),
            patch_version,
            owner_account: account.to_string(),
            package_id: package_id.clone(),
            package_arn: arn_for("package", &package_id, account, region),
            tags: None,
        }
    }

    /// moto's `Package.response_object()` (describe / listed shape).
    fn described(&self, account: u64, region: &str) -> Value {
        let mut out = json!({
            "Category": self.category,
            "CreatedTime": iso(self.created),
            "Description": self.description,
            "LastUpdatedTime": iso(self.last_updated),
            "Name": self.name,
            "OutputPackageName": self.output_package_name,
            "OwnerAccount": self.owner_account,
            "PackageId": self.package_id,
            "PackageArn": self.package_arn,
            "PackageName": self.package_name,
            "PackageVersion": self.package_version,
            "PatchVersion": self.patch_version,
            "StorageLocation": storage_location(account, region),
        });
        if let Some(t) = &self.tags {
            out["Tags"] = tags_json(t);
        }
        out
    }
}

/// moto's `Node` (a package-from-template job). `node_description` holds
/// `node_name` — moto's argument slip, visible on the wire.
struct Node {
    job_id: String,
    created: DateTime<Utc>,
    last_updated: DateTime<Utc>,
    job_tags: Option<Value>,
    node_name: String,
    output_package_name: String,
    output_package_version: String,
    template_parameters: Option<Value>,
    template_type: String,
}

impl Node {
    /// moto's `Node.response_object()` (describe) — no null-filtering, so
    /// unset members ride along as `null` exactly like moto's dicts.
    fn described(&self) -> Value {
        json!({
            "CreatedTime": iso(self.created),
            "JobId": self.job_id,
            "JobTags": self.job_tags,
            "LastUpdatedTime": iso(self.last_updated),
            "NodeDescription": self.node_name,
            "NodeName": self.node_name,
            "OutputPackageName": self.output_package_name,
            "OutputPackageVersion": self.output_package_version,
            "Status": "PENDING",
            "TemplateParameters": self.template_parameters,
            "TemplateType": self.template_type,
        })
    }

    /// moto's `Node.response_listed()`.
    fn listed(&self) -> Value {
        json!({
            "CreatedTime": iso(self.created),
            "JobId": self.job_id,
            "NodeName": self.node_name,
            "Status": "PENDING",
            "TemplateType": self.template_type,
        })
    }
}

/// moto's `Node.protect_secrets` — password/username keys are masked before
/// storage and responses.
fn protect_secrets(params: Option<Value>) -> Option<Value> {
    let obj = params?.as_object()?.clone();
    let mut masked = Map::new();
    for (k, v) in obj {
        let lower = k.to_ascii_lowercase();
        masked.insert(
            k,
            if lower == "password" || lower == "username" {
                json!("SAVED_AS_SECRET")
            } else {
                v
            },
        );
    }
    Some(Value::Object(masked))
}

struct PackageImportJob {
    job_id: String,
    client_token: Option<Value>,
    input_config: Option<Value>,
    job_type: Option<String>,
    output_config: Option<Value>,
    job_tags: Option<Value>,
    created: DateTime<Utc>,
    last_updated: DateTime<Utc>,
    output: Value,
}

impl PackageImportJob {
    fn new(account: u64, region: &str, body: &Value) -> Self {
        let job_id = uuid::Uuid::new_v4().to_string();
        let output = json!({
            "OutputS3Location": {
                "BucketName": format!("panorama-{region}"),
                "ObjectKey": format!("{account}/packages/{job_id}"),
            },
            "PackageId": format!("package-{}", hash_name(&job_id)),
            "PackageVersion": "1.0",
            "PatchVersion": &generate_package_id(&job_id)[..16],
        });
        PackageImportJob {
            job_id,
            client_token: body.get("ClientToken").cloned(),
            input_config: body.get("InputConfig").cloned(),
            job_type: body
                .get("JobType")
                .and_then(Value::as_str)
                .map(String::from),
            output_config: body.get("OutputConfig").cloned(),
            job_tags: body.get("JobTags").cloned(),
            created: Utc::now(),
            last_updated: Utc::now(),
            output,
        }
    }

    /// moto's `PackageImportJob.response_describe()`.
    fn described(&self) -> Value {
        let mut out = json!({
            "ClientToken": self.client_token,
            "CreatedTime": iso(self.created),
            "InputConfig": self.input_config,
            "JobId": self.job_id,
            "JobType": self.job_type,
            "LastUpdatedTime": iso(self.last_updated),
            "Output": self.output,
            "OutputConfig": self.output_config,
            "Status": "SUCCEEDED",
            "StatusMessage": "",
        });
        if let Some(Value::Array(tags)) = &self.job_tags {
            if !tags.is_empty() {
                out["JobTags"] = json!(tags);
            }
        }
        out
    }

    /// moto's `PackageImportJob.response_listed()`.
    fn listed(&self) -> Value {
        json!({
            "CreatedTime": iso(self.created),
            "JobId": self.job_id,
            "JobType": self.job_type,
            "LastUpdatedTime": iso(self.last_updated),
            "Status": "SUCCEEDED",
            "StatusMessage": "",
        })
    }
}

struct ApplicationInstance {
    application_instance_id: String,
    arn: String,
    default_runtime_context_device: String,
    default_runtime_context_device_name: String,
    description: Option<Value>,
    manifest_overrides_payload: Option<Value>,
    manifest_payload: Option<Value>,
    name: Option<Value>,
    runtime_role_arn: Option<Value>,
    tags: Option<Value>,
    created: DateTime<Utc>,
    last_updated: DateTime<Utc>,
    runtime_context_states: Vec<Value>,
}

impl ApplicationInstance {
    fn new(account: u64, region: &str, body: &Value, device_name: &str) -> Self {
        let now = Utc::now();
        // moto: `name = f"{self.name}-{self.created_time}"` fed to hash_name,
        // then lowercased — an absent name renders as the literal "None".
        let id_input = format!(
            "{}-{}",
            body.get("Name").and_then(Value::as_str).unwrap_or("None"),
            dt_space(now)
        );
        let application_instance_id = format!(
            "applicationInstance-{}",
            hash_name(&id_input).to_lowercase()
        );
        ApplicationInstance {
            arn: arn_for(
                "application-instance",
                &application_instance_id,
                account,
                region,
            ),
            application_instance_id,
            default_runtime_context_device: body
                .get("DefaultRuntimeContextDevice")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            default_runtime_context_device_name: device_name.to_string(),
            description: body.get("Description").cloned(),
            manifest_overrides_payload: body.get("ManifestOverridesPayload").cloned(),
            manifest_payload: body.get("ManifestPayload").cloned(),
            name: body.get("Name").cloned(),
            runtime_role_arn: body.get("RuntimeRoleArn").cloned(),
            tags: body.get("Tags").cloned(),
            created: now,
            last_updated: now,
            runtime_context_states: vec![json!({
                "DesiredState": "RUNNING",
                "DeviceReportedStatus": "RUNNING",
                "DeviceReportedTime": iso(now),
                "RuntimeContextName": "string",
            })],
        }
    }

    /// moto's `add_new_runtime_context_states` (replacements & removals).
    fn add_context_state(&mut self, desired: &str, reported: &str) {
        self.runtime_context_states.push(json!({
            "DesiredState": desired,
            "DeviceReportedStatus": reported,
            "DeviceReportedTime": iso(Utc::now()),
            "RuntimeContextName": "string",
        }));
    }

    /// moto's `ApplicationInstance.response_object()` (describe & listed).
    fn described(&self) -> Value {
        json!({
            "ApplicationInstanceId": self.application_instance_id,
            "Arn": self.arn,
            "CreatedTime": iso(self.created),
            "DefaultRuntimeContextDevice": self.default_runtime_context_device,
            "DefaultRuntimeContextDeviceName": self.default_runtime_context_device_name,
            "Description": self.description,
            "HealthStatus": "RUNNING",
            "LastUpdatedTime": iso(self.last_updated),
            "ManifestOverridesPayload": self.manifest_overrides_payload,
            "ManifestPayload": self.manifest_payload,
            "Name": self.name,
            "RuntimeContextStates": self.runtime_context_states,
            "RuntimeRoleArn": self.runtime_role_arn,
            "Status": "DEPLOYMENT_SUCCEEDED",
            "StatusDescription": "string",
            "Tags": self.tags,
        })
    }
}

struct DeviceJob {
    job_id: String,
    device_id: String,
    device_name: String,
    device_arn: String,
    device_type: String,
    job_type: Option<String>,
    image_version: String,
    created: DateTime<Utc>,
}

impl DeviceJob {
    /// moto's `DeviceJob.response_object()` (describe).
    fn described(&self) -> Value {
        json!({
            "CreatedTime": iso(self.created),
            "DeviceArn": self.device_arn,
            "DeviceId": self.device_id,
            "DeviceName": self.device_name,
            "DeviceType": self.device_type,
            "ImageVersion": self.image_version,
            "JobId": self.job_id,
            "JobType": self.job_type,
            "Status": "COMPLETED",
        })
    }

    /// moto's `DeviceJob.response_listed()`.
    fn listed(&self) -> Value {
        json!({
            "CreatedTime": iso(self.created),
            "DeviceId": self.device_id,
            "DeviceName": self.device_name,
            "JobId": self.job_id,
            "JobType": self.job_type,
            "Status": "COMPLETED",
        })
    }
}

struct PackageVersion {
    owner_account: String,
    package_arn: String,
    package_id: String,
    package_name: String,
    package_version: String,
    patch_version: String,
    /// moto's `mark_latest` flag as received (default false).
    mark_latest: bool,
    registered: DateTime<Utc>,
}

impl PackageVersion {
    /// moto's `PackageVersion.response_object()`.
    fn described(&self) -> Value {
        json!({
            "IsLatestPatch": self.mark_latest,
            "OwnerAccount": self.owner_account,
            "PackageArn": self.package_arn,
            "PackageId": self.package_id,
            "PackageName": self.package_name,
            "PackageVersion": self.package_version,
            "PatchVersion": self.patch_version,
            "RegisteredTime": iso(self.registered),
            "Status": "REGISTER_COMPLETED",
            "StatusDescription": "",
        })
    }
}

// ---------------------------------------------------------------------------
// Handlers — ordered like the fork's responses.py
// ---------------------------------------------------------------------------

fn provision_device(st: &mut PanoramaStore, region: &str, account: u64, body: &Value) -> Response {
    let Some(name) = body_str(body, "Name") else {
        return validation("Name is required".to_string());
    };
    let mut device = Device::new(account, region, &name);
    device.description = body_str(body, "Description");
    device.network_configuration = body.get("NetworkingConfiguration").cloned();
    device.tags = tags_from(body.get("Tags"));
    st.devices.push(device);
    // moto's ManagedState defaults to "immediate" progression, so every read
    // yields the terminal statuses: provision's Status is always SUCCEEDED.
    ok_json(json!({
        "Arn": arn_for("device", &name, account, region),
        "Certificates": base64_certificate(),
        "DeviceId": format!("device-{}", hash_name(&name)),
        "IotThingName": "",
        "Status": "SUCCEEDED",
    }))
}

fn list_devices(st: &mut PanoramaStore, query: &BTreeMap<String, Vec<String>>) -> Response {
    let (max_results, next_token) = list_params(query);
    let name_filter = query_str(query, "NameFilter");
    let status_filter = query_str(query, "DeviceAggregatedStatusFilter");
    let sort_by = query_str(query, "SortBy");
    let sort_order = query_str(query, "SortOrder");

    // Devices only ever report the terminal aggregated status (moto's
    // immediate progression), so a mismatching DeviceAggregatedStatusFilter
    // produces an empty list.
    if status_filter.as_deref().is_some_and(|f| f != "ONLINE") {
        return list_response("Devices", Vec::new(), None);
    }

    let mut listed: Vec<&Device> = st
        .devices
        .iter()
        .filter(|d| {
            name_filter
                .as_deref()
                .map(|f| d.name.starts_with(f))
                .unwrap_or(true)
        })
        .collect();
    // moto's sort-key table; an absent/unknown SortBy falls back to
    // created_time. DEVICE_AGGREGATED_STATUS keys on the (constant) status,
    // which compares equal for every device — moto's unstable sort keeps a
    // deterministic order, and the id tiebreak reproduces it here.
    let key = match sort_by.as_deref() {
        Some("DEVICE_ID") => 1,
        Some("NAME") => 2,
        Some("DEVICE_AGGREGATED_STATUS") => 3,
        _ => 4,
    };
    let ascending = sort_order.as_deref() != Some("DESCENDING");
    listed.sort_by(|a, b| {
        let ord = match key {
            1 | 3 => a.device_id.cmp(&b.device_id),
            2 => a.name.cmp(&b.name),
            _ => a.created.cmp(&b.created),
        };
        if ascending {
            ord
        } else {
            ord.reverse()
        }
    });
    let items: Vec<Value> = listed.iter().map(|d| d.listed()).collect();
    match paginate(items, max_results, next_token, 123, "DeviceId") {
        Ok((page, token)) => list_response("Devices", page, token),
        Err(resp) => resp,
    }
}

fn describe_device(st: &mut PanoramaStore, device_id: &str) -> Response {
    let Some(device) = st.devices.iter().find(|d| d.device_id == device_id) else {
        return validation(format!("Device {device_id} not found"));
    };
    let mut out = device.described();
    out["ProvisioningStatus"] = json!("SUCCEEDED");
    ok_json(out)
}

fn update_device_metadata(st: &mut PanoramaStore, device_id: &str, body: &Value) -> Response {
    let Some(pos) = st.devices.iter().position(|d| d.device_id == device_id) else {
        return internal_error(device_id);
    };
    let device = &mut st.devices[pos];
    device.description = body_str(body, "Description");
    device.last_updated = Utc::now();
    ok_json(json!({ "DeviceId": device_id }))
}

fn delete_device(st: &mut PanoramaStore, device_id: &str) -> Response {
    let Some(pos) = st.devices.iter().position(|d| d.device_id == device_id) else {
        return internal_error(device_id);
    };
    st.devices.remove(pos);
    ok_json(json!({ "DeviceId": device_id }))
}

fn create_node_from_template_job(
    st: &mut PanoramaStore,
    region: &str,
    account: u64,
    body: &Value,
) -> Response {
    let job_id = uuid::Uuid::new_v4().to_string();
    let node_name = body_str(body, "NodeName").unwrap_or_default();
    let output_package_name = body_str(body, "OutputPackageName").unwrap_or_default();
    let output_package_version = body_str(body, "OutputPackageVersion").unwrap_or_default();
    let template_type = body_str(body, "TemplateType").unwrap_or_default();
    st.node_jobs.push(Node {
        job_id: job_id.clone(),
        created: Utc::now(),
        last_updated: Utc::now(),
        job_tags: body.get("JobTags").cloned(),
        node_name: node_name.clone(),
        output_package_name: output_package_name.clone(),
        output_package_version: output_package_version.clone(),
        template_parameters: protect_secrets(body.get("TemplateParameters").cloned()),
        template_type: template_type.clone(),
    });
    // moto seeds a MEDIA_SOURCE package (a "node") for RTSP camera streams.
    if template_type == "RTSP_CAMERA_STREAM" {
        let node_description = body.get("NodeDescription").and_then(Value::as_str);
        let package = Package::new(
            account,
            region,
            &node_name,
            &output_package_name,
            &output_package_version,
            node_description.map(String::from),
            "MEDIA_SOURCE",
        );
        st.nodes.push(package);
    }
    ok_json(json!({ "JobId": job_id }))
}

fn describe_node_from_template_job(st: &mut PanoramaStore, job_id: &str) -> Response {
    match st.node_jobs.iter().find(|j| j.job_id == job_id) {
        Some(job) => ok_json(job.described()),
        None => not_found(format!("Job {job_id} not found")),
    }
}

fn list_template_jobs(st: &mut PanoramaStore) -> Response {
    // The fork reads `maxResults`/`nextToken` lowercase — botocore sends
    // capitalised keys, so these lists are always complete and last-page.
    let items: Vec<Value> = st.node_jobs.iter().map(Node::listed).collect();
    list_response("NodeFromTemplateJobs", items, None)
}

fn list_nodes() -> Response {
    // The fork reads `category` (lowercase) from the query — botocore sends
    // `Category`, so the `x.category == category` filter never matches and
    // the list stays empty.
    list_response("Nodes", Vec::new(), None)
}

/// moto's `describe_node` renders the Package response_object; the storage
/// location derives from the backend (account, region) in scope — the
/// caller's —, not the package's creation-time region. Mirror that.
fn describe_node(st: &mut PanoramaStore, region: &str, account: u64, node_id: &str) -> Response {
    match st.nodes.iter().find(|n| n.package_id == node_id) {
        Some(node) => ok_json(node.described(account, region)),
        None => not_found(format!("Node {node_id} not found")),
    }
}

fn create_package(st: &mut PanoramaStore, region: &str, account: u64, body: &Value) -> Response {
    let Some(package_name) = body_str(body, "PackageName") else {
        return validation("PackageName is required".to_string());
    };
    let mut package = Package::new(
        account,
        region,
        &package_name,
        &package_name,
        "1.0",
        Some(String::new()),
        "BUSINESS_LOGIC",
    );
    package.tags = tags_from(body.get("Tags"));
    let response = json!({
        "Arn": package.package_arn,
        "PackageId": package.package_id,
        "StorageLocation": storage_location(account, region),
    });
    st.packages.push(package);
    ok_json(response)
}

fn describe_package(
    st: &mut PanoramaStore,
    region: &str,
    account: u64,
    package_id: &str,
) -> Response {
    match st.packages.iter().find(|p| p.package_id == package_id) {
        Some(package) => ok_json(package.described(account, region)),
        None => not_found(format!("Package {package_id} not found")),
    }
}

fn delete_package(st: &mut PanoramaStore, package_id: &str) -> Response {
    let Some(pos) = st.packages.iter().position(|p| p.package_id == package_id) else {
        return not_found(format!("Package {package_id} not found"));
    };
    st.packages.remove(pos);
    ok_json(json!({}))
}

fn list_packages(st: &mut PanoramaStore, region: &str, account: u64) -> Response {
    let items: Vec<Value> = st
        .packages
        .iter()
        .map(|p| p.described(account, region))
        .collect();
    list_response("Packages", items, None)
}

fn create_package_import_job(
    st: &mut PanoramaStore,
    region: &str,
    account: u64,
    body: &Value,
) -> Response {
    let job = PackageImportJob::new(account, region, body);
    let job_id = job.job_id.clone();
    st.package_import_jobs.push(job);
    ok_json(json!({ "JobId": job_id }))
}

fn describe_package_import_job(st: &mut PanoramaStore, job_id: &str) -> Response {
    match st.package_import_jobs.iter().find(|j| j.job_id == job_id) {
        Some(job) => ok_json(job.described()),
        None => not_found(format!("PackageImportJob {job_id} not found")),
    }
}

fn list_package_import_jobs(st: &mut PanoramaStore) -> Response {
    let items: Vec<Value> = st.package_import_jobs.iter().map(|j| j.listed()).collect();
    list_response("PackageImportJobs", items, None)
}

fn create_application_instance(
    st: &mut PanoramaStore,
    region: &str,
    account: u64,
    body: &Value,
) -> Response {
    let device_id = body
        .get("DefaultRuntimeContextDevice")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let Some(device_pos) = st.devices.iter().position(|d| d.device_id == device_id) else {
        return validation(format!("Device {device_id} not found"));
    };
    let device_name = st.devices[device_pos].name.clone();
    // moto: a replaced instance is marked REMOVAL_SUCCEEDED first.
    if let Some(replace_id) = body
        .get("ApplicationInstanceIdToReplace")
        .and_then(Value::as_str)
    {
        if let Some(existing) = st
            .application_instances
            .iter_mut()
            .find(|ai| ai.application_instance_id == replace_id)
        {
            existing.add_context_state("REMOVED", "REMOVAL_IN_PROGRESS");
        }
    }
    let instance = ApplicationInstance::new(account, region, body, &device_name);
    let application_instance_id = instance.application_instance_id.clone();
    st.application_instances.push(instance);
    ok_json(json!({ "ApplicationInstanceId": application_instance_id }))
}

fn describe_application_instance(
    st: &mut PanoramaStore,
    application_instance_id: &str,
) -> Response {
    match st
        .application_instances
        .iter()
        .find(|ai| ai.application_instance_id == application_instance_id)
    {
        Some(ai) => ok_json(ai.described()),
        None => not_found(format!(
            "ApplicationInstance {application_instance_id} not found"
        )),
    }
}

fn list_application_instances(st: &mut PanoramaStore) -> Response {
    let items: Vec<Value> = st
        .application_instances
        .iter()
        .map(ApplicationInstance::described)
        .collect();
    list_response("ApplicationInstances", items, None)
}

fn remove_application_instance(
    st: &mut PanoramaStore,
    region: &str,
    account: u64,
    application_instance_id: &str,
) -> Response {
    let Some(pos) = st
        .application_instances
        .iter()
        .position(|ai| ai.application_instance_id == application_instance_id)
    else {
        return not_found(format!(
            "ApplicationInstance {application_instance_id} not found"
        ));
    };
    st.application_instances.remove(pos);
    let _ = (region, account);
    ok_json(json!({}))
}

fn list_dependencies(st: &mut PanoramaStore, application_instance_id: &str) -> Response {
    if !st
        .application_instances
        .iter()
        .any(|ai| ai.application_instance_id == application_instance_id)
    {
        return not_found(format!(
            "ApplicationInstance {application_instance_id} not found"
        ));
    }
    // moto resolves dependencies from the manifest — always empty.
    list_response("PackageObjects", Vec::new(), None)
}

fn list_node_instances(st: &mut PanoramaStore, application_instance_id: &str) -> Response {
    if !st
        .application_instances
        .iter()
        .any(|ai| ai.application_instance_id == application_instance_id)
    {
        return not_found(format!(
            "ApplicationInstance {application_instance_id} not found"
        ));
    }
    // Node instances are populated from runtime — always empty.
    list_response("NodeInstances", Vec::new(), None)
}

fn signal_node_instances(st: &mut PanoramaStore, application_instance_id: &str) -> Response {
    if !st
        .application_instances
        .iter()
        .any(|ai| ai.application_instance_id == application_instance_id)
    {
        return not_found(format!(
            "ApplicationInstance {application_instance_id} not found"
        ));
    }
    // Signals are accepted; no state change in moto's stub.
    ok_json(json!({ "ApplicationInstanceId": application_instance_id }))
}

fn create_job_for_devices(
    st: &mut PanoramaStore,
    region: &str,
    account: u64,
    body: &Value,
) -> Response {
    let job_type = body
        .get("JobType")
        .and_then(Value::as_str)
        .map(String::from);
    let image_version = body
        .get("DeviceJobConfig")
        .and_then(|c| c.get("OTAJobConfig"))
        .and_then(|c| c.get("ImageVersion"))
        .and_then(Value::as_str)
        .map(String::from);
    let mut created = Vec::new();
    if let Some(device_ids) = body.get("DeviceIds").and_then(Value::as_array) {
        for device_id in device_ids.iter().filter_map(Value::as_str) {
            let Some(device_pos) = st.devices.iter().position(|d| d.device_id == device_id) else {
                return validation(format!("Device {device_id} not found"));
            };
            let device = &st.devices[device_pos];
            let job = DeviceJob {
                job_id: uuid::Uuid::new_v4().to_string(),
                device_id: device_id.to_string(),
                device_name: device.name.clone(),
                device_arn: arn_for("device", device_id, account, region),
                // moto passes the device's type attribute.
                device_type: "PANORAMA_APPLIANCE".to_string(),
                job_type: job_type.clone(),
                image_version: image_version.clone().unwrap_or_else(|| "1.0.0".to_string()),
                created: Utc::now(),
            };
            created.push(json!({
                "DeviceId": job.device_id,
                "JobId": job.job_id,
            }));
            st.device_jobs.push(job);
        }
    }
    ok_json(json!({ "Jobs": created }))
}

fn describe_device_job(st: &mut PanoramaStore, job_id: &str) -> Response {
    match st.device_jobs.iter().find(|j| j.job_id == job_id) {
        Some(job) => ok_json(job.described()),
        None => not_found(format!("DeviceJob {job_id} not found")),
    }
}

fn list_devices_jobs(st: &mut PanoramaStore, query: &BTreeMap<String, Vec<String>>) -> Response {
    let (max_results, next_token) = list_params(query);
    let device_id = query_str(query, "DeviceId");
    let items: Vec<Value> = st
        .device_jobs
        .iter()
        .filter(|j| {
            device_id
                .as_deref()
                .map(|d| j.device_id == d)
                .unwrap_or(true)
        })
        .map(|j| j.listed())
        .collect();
    match paginate(items, max_results, next_token, 25, "JobId") {
        Ok((page, token)) => list_response("DeviceJobs", page, token),
        Err(resp) => resp,
    }
}

fn register_package_version(
    st: &mut PanoramaStore,
    region: &str,
    account: u64,
    package_id: &str,
    package_version: &str,
    patch_version: &str,
    body: &Value,
) -> Response {
    let mark_latest = body
        .get("MarkLatest")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let package_name = st
        .packages
        .iter()
        .find(|p| p.package_id == package_id)
        .map(|p| p.package_name.clone())
        .unwrap_or_else(|| package_id.to_string());
    let key = format!("{package_id}/{package_version}/{patch_version}");
    st.package_versions.insert(
        key,
        PackageVersion {
            owner_account: account.to_string(),
            package_arn: arn_for("package", package_id, account, region),
            package_id: package_id.to_string(),
            package_name,
            package_version: package_version.to_string(),
            patch_version: patch_version.to_string(),
            mark_latest,
            registered: Utc::now(),
        },
    );
    ok_json(json!({}))
}

fn deregister_package_version(
    st: &mut PanoramaStore,
    package_id: &str,
    package_version: &str,
    patch_version: &str,
) -> Response {
    let key = format!("{package_id}/{package_version}/{patch_version}");
    match st.package_versions.remove(&key) {
        Some(_) => ok_json(json!({})),
        None => not_found(format!(
            "PackageVersion {package_id}/{package_version}/{patch_version} not found"
        )),
    }
}

fn describe_package_version(
    st: &mut PanoramaStore,
    region: &str,
    account: u64,
    package_id: &str,
    package_version: &str,
    patch_version: Option<String>,
) -> Response {
    // The fork reads `ownerAccount` lowercase — botocore sends `OwnerAccount`
    // — so the owner is always the caller's account (kept faithful).
    let _ = (region, account);
    if let Some(patch) = patch_version.filter(|p| !p.is_empty()) {
        let key = format!("{package_id}/{package_version}/{patch}");
        return match st.package_versions.get(&key) {
            Some(pv) => ok_json(pv.described()),
            None => not_found(format!(
                "PackageVersion {package_id}/{package_version} not found"
            )),
        };
    }
    // Without a patch version moto scans for the package/version pair.
    let found = st
        .package_versions
        .values()
        .find(|pv| pv.package_id == package_id && pv.package_version == package_version);
    match found {
        Some(pv) => ok_json(pv.described()),
        None => not_found(format!(
            "PackageVersion {package_id}/{package_version} not found"
        )),
    }
}

// ---------------------------------------------------------------------------
// Tags
// ---------------------------------------------------------------------------

/// moto's `_get_tags_for_arn`: the `_tags` store overlaid with built-in
/// resource tags (devices / packages / application instances only).
fn tags_for_arn(st: &PanoramaStore, arn: &str) -> BTreeMap<String, String> {
    let mut tags = st.tags.get(arn).cloned().unwrap_or_default();
    for device in &st.devices {
        if device.arn == arn {
            if let Some(device_tags) = &device.tags {
                for (k, v) in device_tags {
                    tags.insert(k.clone(), v.clone());
                }
            }
        }
    }
    for package in &st.packages {
        if package.package_arn == arn {
            if let Some(package_tags) = &package.tags {
                for (k, v) in package_tags {
                    tags.insert(k.clone(), v.clone());
                }
            }
        }
    }
    for ai in &st.application_instances {
        if ai.arn == arn {
            tags.extend(tags_flat(ai.tags.as_ref().unwrap_or(&Value::Null)));
        }
    }
    tags
}

fn tags_flat(v: &Value) -> Vec<(String, String)> {
    v.as_object()
        .map(|m| {
            m.iter()
                .filter_map(|(k, val)| val.as_str().map(|s| (k.clone(), s.to_string())))
                .collect()
        })
        .unwrap_or_default()
}

fn list_tags_for_resource(st: &mut PanoramaStore, resource_arn: &str) -> Response {
    let tags = tags_for_arn(st, resource_arn);
    ok_json(json!({ "Tags": tags_json(&tags) }))
}

fn tag_resource(st: &mut PanoramaStore, resource_arn: &str, body: &Value) -> Response {
    let mut existing = tags_for_arn(st, resource_arn);
    if let Some(new_tags) = tags_from(body.get("Tags")) {
        existing.extend(new_tags);
    }
    st.tags.insert(resource_arn.to_string(), existing);
    ok_json(json!({}))
}

/// moto reads `tagKeys` from the querystring (repeated params).
fn untag_resource(
    st: &mut PanoramaStore,
    resource_arn: &str,
    query: &BTreeMap<String, Vec<String>>,
) -> Response {
    let mut existing = tags_for_arn(st, resource_arn);
    for key in query.get("tagKeys").cloned().unwrap_or_default() {
        existing.remove(&key);
    }
    st.tags.insert(resource_arn.to_string(), existing);
    ok_json(json!({}))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_name_matches_moto_shape() {
        // urlsafe, unpadded base64 for a 16-byte md5 digest → 22 chars.
        let h = hash_name("my-device");
        assert_eq!(h.len(), 22);
        assert!(!h.contains('='));
        assert!(h
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'));
        assert_ne!(hash_name("other"), h);
    }

    #[test]
    fn generate_package_id_matches_moto_shape() {
        let id = generate_package_id("some-package");
        assert_eq!(id.len(), 64);
        assert!(id
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit()));
    }

    #[test]
    fn paginate_slices_pages_and_tokens() {
        let items: Vec<Value> = (0..5)
            .map(|i| json!({ "DeviceId": format!("device-{i}") }))
            .collect();
        let (page, token) = paginate(items.clone(), Some(2), None, 123, "DeviceId").unwrap();
        assert_eq!(page.len(), 2);
        let token = token.expect("more pages follow");

        let (page2, token2) =
            paginate(items.clone(), Some(2), Some(token), 123, "DeviceId").unwrap();
        assert_eq!(page2[0]["DeviceId"], json!("device-2"));
        assert!(token2.is_some());

        // The final page carries no token.
        let (page3, token3) =
            paginate(items, Some(2), Some(token2.unwrap()), 123, "DeviceId").unwrap();
        assert_eq!(page3.len(), 1);
        assert!(token3.is_none());
    }

    #[test]
    fn paginate_rejects_bad_tokens() {
        let items = vec![json!({ "DeviceId": "device-x" })];
        let resp = paginate(
            items.clone(),
            None,
            Some("!!!not-base64".to_string()),
            123,
            "DeviceId",
        )
        .unwrap_err();
        assert_eq!(resp.status, 400);
        let resp =
            paginate(items, None, Some("cGFnZQ==".to_string()), 123, "DeviceId").unwrap_err();
        assert_eq!(resp.status, 400);
    }

    #[test]
    fn protect_secrets_masks_credentials() {
        let masked = protect_secrets(Some(json!({
            "Password": "x",
            "Username": "y",
            "other": "keep",
        })))
        .unwrap();
        assert_eq!(masked["Password"], json!("SAVED_AS_SECRET"));
        assert_eq!(masked["Username"], json!("SAVED_AS_SECRET"));
        assert_eq!(masked["other"], json!("keep"));
        assert!(protect_secrets(None).is_none());
    }
}
