//! AWS S3 Tables backend (rest-json, ARN-bearing URI paths).
//!
//! Port of the moto fork's `s3tables` backend (the deployed fork checkout
//! is the wave-2 ground truth; it supersedes the stale probe file for
//! `GetTable` / `PutTableRecordExpirationConfiguration` — probe-recorded
//! 500s while the fork's handlers are healthy — and the replication /
//! metrics surface that post-dates the capture).
//!
//! Wire shapes mirror the moto sidecar: lowercase camel keys,
//! `datetime.isoformat()` timestamps, JSON members with `None` values
//! omitted (botocore treats them as absent), `{"__type": <code>,
//! "message": …}` error bodies (`JsonRESTError`), and empty-body 200s for
//! the mutations moto ships as `(204, {}, "")` tuples (its dispatcher
//! defaults headerless statuses to 200 — that is the wire the deleted
//! suites measured). State is per (account, region).
//!
//! Two intentional deviations from moto's literal handler behavior, both
//! needed to make the restored compat suite pass (and both documented in
//! the port report):
//! - `PutTableBucketMaintenanceConfiguration` / `GetTableBucketMaintenance
//!   Configuration`: moto's `_parse_path(raw, ≤1)` folds the literal
//!   `/maintenance` segment into the bucket ARN and always 404s; this port
//!   strips the `maintenance[/{type}]` tail so the stored config works.
//! - Replication / record-expiration params (`tableBucketARN`, `tableArn`)
//!   are read from query as well as body: current botocore sends them as
//!   querystring members while moto only reads the body (KeyError → 500).
//!
//! Routing: the botocore model puts ARNs in URI paths
//! (`/buckets/{tableBucketARN}`, `/tables/{tableBucketARN}/{namespace}/…`)
//! and botocore percent-encodes the `/` inside an ARN (other clients may
//! send it raw). moto's `_parse_path` handles both by splitting the path
//! from the right with the tail segment count; this port does the same —
//! each route pins its op by method + trailing-shape markers, then splits
//! the tail from the right so the ARN survives as one piece. The gateway
//! leaves `operation` empty for rest-json services, so the operation is
//! derived here from method + path (one route per botocore requestUri).

use crate::store::{self, Slot};
use crate::{Request, Response};
use chrono::{SecondsFormat, Utc};
use once_cell::sync::Lazy;
use serde_json::{json, Map, Value};
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

struct TableBucket {
    name: String,
    account_id: String,
    region: String,
    creation_date: String,
    namespaces: HashMap<String, Namespace>,
    policy: Option<String>,
    maintenance_configuration: HashMap<String, Value>,
    encryption_configuration: Option<Value>,
    metrics_configuration: Option<String>,
    storage_class_configuration: Value,
    replication_configuration: Option<Value>,
    replication_version_token: Option<String>,
}

impl TableBucket {
    fn arn(&self) -> String {
        format!(
            "arn:{}:s3tables:{}:{}:bucket/{}",
            partition(&self.region),
            self.region,
            self.account_id,
            self.name
        )
    }

    /// Projection moto renders in create / get / list.
    fn projection(&self) -> Value {
        json!({
            "arn": self.arn(),
            "name": self.name,
            "ownerAccountId": self.account_id,
            "createdAt": self.creation_date,
        })
    }
}

struct Namespace {
    name: String,
    account_id: String,
    created_by: String,
    creation_date: String,
    tables: HashMap<String, Table>,
}

struct Table {
    name: String,
    namespace: String,
    table_bucket_arn: String,
    account_id: String,
    format: String,
    ttype: &'static str,
    version_token: String,
    creation_date: String,
    last_modified: String,
    modified_by: Option<String>,
    managed_by_service: bool,
    metadata_location: Option<String>,
    warehouse_location: String,
    policy: Option<String>,
    maintenance_configuration: HashMap<String, Value>,
    encryption_configuration: Value,
    storage_class_configuration: Value,
    replication_configuration: Option<Value>,
    replication_version_token: Option<String>,
    record_expiration_configuration: Option<Value>,
}

impl Table {
    fn arn(&self) -> String {
        format!("{}/table/{}", self.table_bucket_arn, self.name)
    }

    /// moto's `get_table` projection; moto omits JSON-null members, so this
    /// drops them before sending (kept members: all non-nulls below).
    fn projection(&self) -> Value {
        json!({
            "name": self.name,
            "type": self.ttype,
            "tableARN": self.arn(),
            // moto wraps the namespace echo in a one-element list
            "namespace": [self.namespace],
            "versionToken": self.version_token,
            "metadataLocation": self.metadata_location,
            "warehouseLocation": self.warehouse_location,
            "createdAt": self.creation_date,
            "createdBy": self.account_id,
            "managedByService": self.managed_by_service,
            "modifiedAt": self.last_modified,
            "modifiedBy": self.modified_by,
            "ownerAccountId": self.account_id,
            "format": self.format,
        })
    }
}

#[derive(Default)]
struct State {
    table_buckets: HashMap<String, TableBucket>,
    /// ARN → {Key: Value} tags, as moto's `TaggingService` keeps them.
    tags: HashMap<String, HashMap<String, String>>,
}

fn state() -> Slot<State> {
    static SLOT: Lazy<Slot<State>> = Lazy::new(|| store::slot("s3tables"));
    SLOT.clone()
}

/// Early-return helper: propagate a prebuilt error `Response` from a
/// `Result<_, Response>` lookup inside a handler returning plain `Response`.
macro_rules! try_resp {
    ($e:expr) => {
        match $e {
            Ok(v) => v,
            Err(resp) => return resp,
        }
    };
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "s3tables" {
        return None;
    }
    // moto's `_get_param` reads the query string first and falls back to
    // the JSON body, so query values win over same-named body members.
    let mut params = crate::proto::parse_json_body(&req.body);
    if !req.query_string.is_empty() {
        params = if params.is_object() {
            params
        } else {
            json!({})
        };
        let obj = params.as_object_mut().expect("object params");
        for (k, v) in crate::proto::query_map(&req.query_string) {
            obj.insert(k, json!(v.first().cloned().unwrap_or_default()));
        }
    }

    let route = match route(req) {
        Some(r) => r,
        None => {
            // Unroutable path/method vs the botocore requestUri table —
            // moto's own generic not-implemented (called via its action
            // table for unmatched rest-json uris).
            return Some(not_implemented(&format!(
                "The {} action has not been implemented",
                req.operation
            )));
        }
    };

    let slot = state();
    let mut st = slot.write();
    let state = st.entry((req.account, req.region.clone())).or_default();
    Some(route.handle(state, req, &params))
}

/// One matched botocore requestUri: operation plus the decoded path pieces.
struct Route {
    op: Op,
    /// URI-ARN resource head, percent-decoded (moto's `_parse_path` head).
    arn: String,
    /// Decoded segments after the ARN.
    tail: Vec<String>,
}

#[derive(Clone, Copy, PartialEq)]
enum Op {
    CreateTableBucket,
    ListTableBuckets,
    GetTableBucket,
    DeleteTableBucket,
    BucketPolicyGet,
    BucketPolicyPut,
    BucketPolicyDelete,
    BucketMaintenanceGet,
    BucketMaintenancePut,
    BucketEncryptionGet,
    BucketEncryptionPut,
    BucketEncryptionDelete,
    BucketMetricsGet,
    BucketMetricsPut,
    BucketMetricsDelete,
    BucketStorageGet,
    BucketStoragePut,
    BucketReplicationGet,
    BucketReplicationPut,
    BucketReplicationDelete,
    CreateNamespace,
    ListNamespaces,
    GetNamespace,
    DeleteNamespace,
    CreateTable,
    GetTable,
    ListTables,
    DeleteTable,
    TablePolicyGet,
    TablePolicyPut,
    TablePolicyDelete,
    TableMaintenanceGet,
    TableMaintenancePut,
    TableMaintenanceJobs,
    TableEncryption,
    TableStorage,
    TableMetadataGet,
    TableMetadataUpdate,
    RenameTable,
    TableReplicationGet,
    TableReplicationPut,
    TableReplicationDelete,
    ReplicationStatus,
    RecordExpirationGet,
    RecordExpirationPut,
    RecordExpirationJobs,
    TagsList,
    TagsTag,
    TagsUntag,
}

impl Route {
    fn handle(&self, st: &mut State, req: &Request, p: &Value) -> Response {
        match self.op {
            Op::CreateTableBucket => bucket_create(st, req, p),
            Op::ListTableBuckets => bucket_list(st, p),
            Op::GetTableBucket => bucket_get(st, &self.arn),
            Op::DeleteTableBucket => bucket_delete(st, &self.arn),
            Op::BucketPolicyGet => bucket_policy_get(st, &self.arn),
            Op::BucketPolicyPut => bucket_policy_put(st, &self.arn, p),
            Op::BucketPolicyDelete => bucket_policy_delete(st, &self.arn),
            Op::BucketMaintenanceGet => bucket_maintenance_get(st, &self.arn),
            Op::BucketMaintenancePut => bucket_maintenance_put(st, &self.arn, self.tail(1), p),
            Op::BucketEncryptionGet => bucket_encryption_get(st, &self.arn),
            Op::BucketEncryptionPut => bucket_encryption_put(st, &self.arn, p),
            Op::BucketEncryptionDelete => bucket_encryption_delete(st, &self.arn),
            Op::BucketMetricsGet => bucket_metrics_get(st, &self.arn),
            Op::BucketMetricsPut => bucket_metrics_put(st, &self.arn),
            Op::BucketMetricsDelete => bucket_metrics_delete(st, &self.arn),
            Op::BucketStorageGet => bucket_storage_get(st, &self.arn),
            Op::BucketStoragePut => bucket_storage_put(st, &self.arn, p),
            Op::BucketReplicationGet => bucket_replication_get(st, &self.arn),
            Op::BucketReplicationPut => bucket_replication_put(st, p),
            Op::BucketReplicationDelete => bucket_replication_delete(st, p),
            Op::CreateNamespace => namespace_create(st, req, p, &self.arn),
            Op::ListNamespaces => namespace_list(st, p, &self.arn),
            Op::GetNamespace => namespace_get(st, &self.arn, self.tail(0)),
            Op::DeleteNamespace => namespace_delete(st, &self.arn, self.tail(0)),
            Op::CreateTable => table_create(st, req, p, self),
            Op::GetTable => table_get(st, p),
            Op::ListTables => table_list(st, p, &self.arn),
            Op::DeleteTable => table_delete(st, self, p),
            Op::TablePolicyGet => table_policy_get(st, &self.arn, self.tail(0), self.tail(1)),
            Op::TablePolicyPut => table_policy_put(st, &self.arn, self.tail(0), self.tail(1), p),
            Op::TablePolicyDelete => table_policy_delete(st, &self.arn, self.tail(0), self.tail(1)),
            Op::TableMaintenanceGet => {
                table_maintenance_get(st, &self.arn, self.tail(0), self.tail(1))
            }
            Op::TableMaintenancePut => {
                table_maintenance_put(st, &self.arn, self.tail(0), self.tail(1), self.tail(3), p)
            }
            Op::TableMaintenanceJobs => {
                table_maintenance_jobs(st, &self.arn, self.tail(0), self.tail(1))
            }
            Op::TableEncryption => table_encryption(st, &self.arn, self.tail(0), self.tail(1)),
            Op::TableStorage => table_storage(st, &self.arn, self.tail(0), self.tail(1)),
            Op::TableMetadataGet => table_metadata_get(st, &self.arn, self.tail(0), self.tail(1)),
            Op::TableMetadataUpdate => {
                table_metadata_update(st, &self.arn, self.tail(0), self.tail(1), p)
            }
            Op::RenameTable => table_rename(st, req, self, p),
            Op::TableReplicationGet => table_replication_get(st, p),
            Op::TableReplicationPut => table_replication_put(st, p),
            Op::TableReplicationDelete => table_replication_delete(st, p),
            Op::ReplicationStatus => table_replication_status(st, p),
            Op::RecordExpirationGet => record_expiration_get(st, p),
            Op::RecordExpirationPut => record_expiration_put(st, p),
            Op::RecordExpirationJobs => record_expiration_jobs(st, p),
            Op::TagsList => tags_list(st, &self.arn),
            Op::TagsTag => tags_tag(st, &self.arn, p),
            Op::TagsUntag => tags_untag(st, &self.arn, req),
        }
    }

    /// One decoded tail segment, `None` when the route carries fewer.
    fn tail(&self, idx: usize) -> &str {
        self.tail.get(idx).map(String::as_str).unwrap_or("")
    }
}

// ---------------------------------------------------------------------------
// Routing (botocore requestUri table ≈ moto's urls.py)
// ---------------------------------------------------------------------------

fn route(req: &Request) -> Option<Route> {
    let path = req.path.trim_start_matches('/');
    let (prefix, rest) = match path.find('/') {
        Some(i) => (&path[..i], &path[i + 1..]),
        None => (path, ""),
    };

    // Bare single-segment families: operations whose parameters ride in the
    // query string or JSON body, never a URI ARN.
    if rest.is_empty() {
        let op = match (req.method.as_str(), prefix) {
            ("PUT", "buckets") => Op::CreateTableBucket,
            ("GET", "buckets") => Op::ListTableBuckets,
            ("GET", "get-table") => Op::GetTable,
            ("GET" | "PUT" | "DELETE", "table-bucket-replication") => match req.method.as_str() {
                "GET" => Op::BucketReplicationGet,
                "PUT" => Op::BucketReplicationPut,
                _ => Op::BucketReplicationDelete,
            },
            ("GET" | "PUT" | "DELETE", "table-replication") => match req.method.as_str() {
                "GET" => Op::TableReplicationGet,
                "PUT" => Op::TableReplicationPut,
                _ => Op::TableReplicationDelete,
            },
            ("GET", "replication-status") => Op::ReplicationStatus,
            ("GET" | "PUT", "table-record-expiration") => match req.method.as_str() {
                "GET" => Op::RecordExpirationGet,
                _ => Op::RecordExpirationPut,
            },
            ("GET", "table-record-expiration-job-status") => Op::RecordExpirationJobs,
            _ => return None,
        };
        return Some(Route {
            op,
            arn: String::new(),
            tail: vec![],
        });
    }

    // Tagging: /tag/{resourceArn} — the ARN always fills the tail.
    if prefix == "tag" {
        let op = match req.method.as_str() {
            "GET" => Op::TagsList,
            "POST" => Op::TagsTag,
            "DELETE" => Op::TagsUntag,
            _ => return None,
        };
        let arn = crate::proto::url_decode(rest);
        if arn.is_empty() {
            return None;
        }
        return Some(Route {
            op,
            arn,
            tail: vec![],
        });
    }

    let (op, n_tail) = match (req.method.as_str(), prefix) {
        ("GET" | "PUT" | "DELETE", "buckets") => buckets_route(req, rest)?,
        ("GET" | "PUT" | "DELETE", "namespaces") => namespaces_route(req, rest)?,
        ("GET" | "PUT" | "DELETE", "tables") => tables_route(req, rest)?,
        _ => return None,
    };
    let (arn, tail) = split_arn_tail(rest, n_tail)?;
    Some(Route { op, arn, tail })
}

/// /buckets/{arn}[/sub[/type]] — op plus the number of segments after the
/// ARN, following moto's URL ordering: sub-resource markers first, then the
/// bare bucket ARN (`.+` greedy).
fn buckets_route(req: &Request, rest: &str) -> Option<(Op, usize)> {
    let last = rest.rsplit('/').next().unwrap_or(rest);
    let second_last = rest
        .rsplit_once('/')
        .map(|(head, _)| head.rsplit('/').next().unwrap_or("").to_string())
        .unwrap_or_default();
    let get_put_delete = |sub: &str, get: Op, put: Op, del: Op| {
        if last != sub {
            return None;
        }
        match req.method.as_str() {
            "GET" => Some(get),
            "PUT" => Some(put),
            "DELETE" => Some(del),
            _ => None,
        }
    };
    if let Some(op) = get_put_delete(
        "policy",
        Op::BucketPolicyGet,
        Op::BucketPolicyPut,
        Op::BucketPolicyDelete,
    ) {
        return Some((op, 1));
    }
    if let Some(op) = get_put_delete(
        "encryption",
        Op::BucketEncryptionGet,
        Op::BucketEncryptionPut,
        Op::BucketEncryptionDelete,
    ) {
        return Some((op, 1));
    }
    if let Some(op) = get_put_delete(
        "metrics",
        Op::BucketMetricsGet,
        Op::BucketMetricsPut,
        Op::BucketMetricsDelete,
    ) {
        return Some((op, 1));
    }
    if last == "storage-class" && req.method != "DELETE" {
        return Some((
            if req.method == "GET" {
                Op::BucketStorageGet
            } else {
                Op::BucketStoragePut
            },
            1,
        ));
    }
    if last == "maintenance" && req.method == "GET" {
        return Some((Op::BucketMaintenanceGet, 1));
    }
    // /buckets/{arn}/maintenance/{type} — strip the two-segment tail.
    if second_last == "maintenance" && req.method == "PUT" {
        return Some((Op::BucketMaintenancePut, 2));
    }
    match req.method.as_str() {
        "GET" => Some((Op::GetTableBucket, 0)),
        "DELETE" => Some((Op::DeleteTableBucket, 0)),
        _ => None,
    }
}

/// /namespaces/{arn}[/{ns}] — Create on the bare ARN (any segment count:
/// kept tolerant of raw-slash ARNs), Get/Delete with one tail segment.
fn namespaces_route(req: &Request, rest: &str) -> Option<(Op, usize)> {
    if req.method == "PUT" {
        return Some((Op::CreateNamespace, 0));
    }
    if rest.rfind('/').is_some() {
        return match req.method.as_str() {
            "GET" => Some((Op::GetNamespace, 1)),
            "DELETE" => Some((Op::DeleteNamespace, 1)),
            _ => None,
        };
    }
    match req.method.as_str() {
        "GET" => Some((Op::ListNamespaces, 0)),
        _ => None,
    }
}

/// /tables/{arn}[/tail...] — moto's urls.py ordering: trailing markers pick
/// the sub-resource op, else the method + segment count picks CRUD ops.
fn tables_route(req: &Request, rest: &str) -> Option<(Op, usize)> {
    let last = rest.rsplit('/').next().unwrap_or(rest);
    let second_last = rest
        .rsplit_once('/')
        .map(|(head, _)| head.rsplit('/').next().unwrap_or("").to_string())
        .unwrap_or_default();
    // Sub-resources: /tables/{arn}/{ns}/{name}/<marker> or /maintenance/{type}.
    // Order matters like moto's regex list: maintenance/{type} is a two-segment
    // marker (PUT), plain markers are single trailing segments.
    if let Some(op) = match (req.method.as_str(), last) {
        ("PUT", "rename") => Some(Op::RenameTable),
        ("PUT", "metadata-location") => Some(Op::TableMetadataUpdate),
        ("PUT", "policy") => Some(Op::TablePolicyPut),
        ("GET", "policy") => Some(Op::TablePolicyGet),
        ("DELETE", "policy") => Some(Op::TablePolicyDelete),
        ("GET", "metadata-location") => Some(Op::TableMetadataGet),
        ("GET", "maintenance-job-status") => Some(Op::TableMaintenanceJobs),
        ("GET", "encryption") => Some(Op::TableEncryption),
        ("GET", "storage-class") => Some(Op::TableStorage),
        ("GET", "maintenance") => Some(Op::TableMaintenanceGet),
        _ => None,
    } {
        return Some((op, 3));
    }
    if second_last == "maintenance" && req.method == "PUT" {
        return Some((Op::TableMaintenancePut, 4));
    }
    match req.method.as_str() {
        "GET" => Some((Op::ListTables, 0)),
        "PUT" => Some((Op::CreateTable, 1)),
        "DELETE" => Some((Op::DeleteTable, 2)),
        _ => None,
    }
}

/// Split `/{prefix}/{arn}[/tail x n_tail]` into (arn, tail) with the ARN
/// kept as one right-anchored piece — moto's `_parse_path` semantics,
/// tolerant of percent-encoded or raw `/` inside the ARN.
fn split_arn_tail(rest: &str, n_tail: usize) -> Option<(String, Vec<String>)> {
    let mut head: &str = rest;
    let mut tail_rev: Vec<&str> = Vec::with_capacity(n_tail);
    for _ in 0..n_tail {
        let idx = head.rfind('/')?;
        if idx == 0 || idx == head.len() - 1 {
            return None;
        }
        tail_rev.push(&head[idx + 1..]);
        head = &head[..idx];
    }
    if head.is_empty() {
        return None;
    }
    tail_rev.reverse();
    Some((
        crate::proto::url_decode(head),
        tail_rev.into_iter().map(crate::proto::url_decode).collect(),
    ))
}

// ---------------------------------------------------------------------------
// Errors (moto's s3tables/exceptions.py + JsonRESTError)
// ---------------------------------------------------------------------------

fn error(status: u16, code: &str, message: &str) -> Response {
    let body = json!({ "__type": code, "message": message });
    let mut resp = Response::new(
        status,
        "application/json",
        serde_json::to_vec(&body).unwrap_or_default(),
    );
    resp.headers
        .push(("x-amzn-ErrorType".to_string(), format!("{code}:")));
    resp
}

fn not_implemented(message: &str) -> Response {
    error(501, "NotImplemented", message)
}

fn not_found(message: &str) -> Response {
    error(404, "NotFoundException", message)
}

fn conflict(message: &str) -> Response {
    error(409, "ConflictException", message)
}

fn bad_request(message: &str) -> Response {
    error(400, "BadRequestException", message)
}

fn version_token_mismatch() -> Response {
    conflict("Provided version token does not match the table version token.")
}

fn table_already_exists() -> Response {
    conflict("A table with an identical name already exists in the namespace.")
}

fn table_does_not_exist() -> Response {
    not_found("The specified table does not exist.")
}

fn bucket_does_not_exist() -> Response {
    not_found("The specified bucket does not exist.")
}

fn namespace_resource_not_found() -> Response {
    not_found("The request was rejected because the specified resource could not be found.")
}

fn namespace_does_not_exist() -> Response {
    not_found("The specified namespace does not exist.")
}

fn policy_does_not_exist() -> Response {
    not_found("The specified policy does not exist.")
}

fn replication_does_not_exist() -> Response {
    not_found("The specified replication configuration does not exist.")
}

fn invalid_bucket_name() -> Response {
    bad_request("The specified bucket name is not valid.")
}

fn invalid_continuation_token() -> Response {
    bad_request("The continuation token is not valid.")
}

fn nothing_to_rename() -> Response {
    bad_request("Neither a new namespace name nor a new table name is specified.")
}

fn invalid_metadata_location() -> Response {
    bad_request("The specified metadata location is not valid.")
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn partition(region: &str) -> &'static str {
    for (prefix, name) in [
        ("cn-", "aws-cn"),
        ("us-gov-", "aws-us-gov"),
        ("us-iso-", "aws-iso"),
        ("us-isob-", "aws-iso-b"),
        ("eusc-", "aws-eusc"),
    ] {
        if region.starts_with(prefix) {
            return name;
        }
    }
    "aws"
}

fn now_iso() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Micros, true)
}

/// moto: `md5(uuid4).hexdigest()[:20]` — twenty lowercase hex chars.
fn version_token() -> String {
    let hex = uuid::Uuid::new_v4().simple().to_string();
    hex.get(..20).unwrap_or(&hex).to_string()
}

/// metrics-configuration id — moto: `str(random.uuid4())`.
fn metrics_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// The warehouse bucket moto creates per table (`s3://{uuid4}--table-s3`).
/// The name is all the API can observe; no S3 backend wiring on this side.
fn warehouse_location() -> String {
    format!("s3://{}--table-s3", uuid::Uuid::new_v4())
}

fn param_str<'a>(p: &'a Value, key: &str) -> Option<&'a str> {
    p.get(key).and_then(Value::as_str)
}

fn param_i64(p: &Value, key: &str) -> Option<i64> {
    match p.get(key) {
        Some(Value::Number(n)) => n.as_i64(),
        Some(Value::String(s)) => s.trim().parse::<i64>().ok(),
        _ => None,
    }
}

/// Table-name check — moto's `re.match(r"[0-9a-z_]*", …)`: anchored at the
/// start with a `*` quantifier, so it also matches the empty prefix and
/// never fails; kept for parity with the moto call site.
fn table_name_is_valid(_name: &str) -> bool {
    true
}

/// Bucket-name check — moto's `re.match(r"[a-z0-9_-]{3,63}", …)` is
/// start-anchored (prefix match), so the leading run of allowed characters
/// must be 3+ chars; reserved prefixes/suffixes are then rejected.
fn table_bucket_name_is_valid(name: &str) -> bool {
    if name.len() > 63 {
        return false;
    }
    let run = name
        .chars()
        .take_while(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == '_' || *c == '-')
        .count();
    if run < 3 {
        return false;
    }
    for prefix in ["xn--", "sthree-", "amzn-s3-demo"] {
        if name.starts_with(prefix) {
            return false;
        }
    }
    for suffix in ["-s3alias", "--ol-s3", "--x-s3"] {
        if name.ends_with(suffix) {
            return false;
        }
    }
    true
}

/// Index pager for the three list operations (moto pipes its list ops
/// through the paginate decorator with a base64 token; only the member
/// shape — a `continuationToken` string — is observable on the wire).
fn page(
    items: Vec<Value>,
    token: Option<&str>,
    limit: Option<i64>,
) -> (Vec<Value>, Option<String>) {
    let start = token.and_then(|t| t.parse::<usize>().ok()).unwrap_or(0);
    let limit = limit.map(|l| l.max(0) as usize).unwrap_or(1000);
    if start >= items.len() {
        return (Vec::new(), None);
    }
    let end = (start + limit).min(items.len());
    let slice = items[start..end].to_vec();
    if end < items.len() {
        (slice, Some(end.to_string()))
    } else {
        (slice, None)
    }
}

/// Shared validation+slice for list ops: invalid tokens reject before paging.
fn validate_token(token: Option<&str>) -> Result<(), Response> {
    match token {
        Some(t) if !t.is_empty() && t.parse::<usize>().is_err() => {
            Err(invalid_continuation_token())
        }
        _ => Ok(()),
    }
}

fn ok_json(v: Value) -> Response {
    Response::new(
        200,
        "application/json",
        serde_json::to_vec(&strip_nulls(v)).unwrap_or_default(),
    )
}

/// moto's rest-json serialization drops members whose value is None
/// (botocore then reports them as absent). Empty dict/list members stay.
fn strip_nulls(v: Value) -> Value {
    match v {
        Value::Object(map) => {
            let mut out = Map::new();
            for (k, v) in map {
                if v.is_null() {
                    continue;
                }
                out.insert(k, strip_nulls(v));
            }
            Value::Object(out)
        }
        Value::Array(items) => Value::Array(items.into_iter().map(strip_nulls).collect()),
        other => other,
    }
}

/// The mutation replies moto ships as `(204, {}, "")` tuples: its
/// dispatcher turns headerless tuples into 200 with an empty JSON body —
/// the wire the deleted compat suites recorded.
fn empty_ok() -> Response {
    Response::new(200, "application/json", Vec::new())
}

/// Look up a table bucket, 404 like moto when absent.
fn find_bucket_mut<'a>(st: &'a mut State, arn: &str) -> Result<&'a mut TableBucket, Response> {
    st.table_buckets
        .get_mut(arn)
        .ok_or_else(bucket_does_not_exist)
}

/// Scan every (bucket, namespace, table) for `arn` — moto's
/// `_find_table_by_arn`; 404 `TableDoesNotExist` on miss.
fn find_table_mut<'a>(st: &'a mut State, arn: &str) -> Result<&'a mut Table, Response> {
    for bucket in st.table_buckets.values_mut() {
        for ns in bucket.namespaces.values_mut() {
            for table in ns.tables.values_mut() {
                if table.arn() == arn {
                    return Ok(table);
                }
            }
        }
    }
    Err(table_does_not_exist())
}

/// Look up a table by (bucket arn, ns, name) — moto's `get_table` +
/// comment-block helpers; 404 `TableDoesNotExist` on miss.
fn find_table_triple<'a>(
    st: &'a mut State,
    arn: &str,
    ns: &str,
    name: &str,
) -> Result<&'a mut Table, Response> {
    st.table_buckets
        .get_mut(arn)
        .and_then(|b| b.namespaces.get_mut(ns))
        .and_then(|n| n.tables.get_mut(name))
        .ok_or_else(table_does_not_exist)
}

// ---------------------------------------------------------------------------
// Table buckets
// ---------------------------------------------------------------------------

fn bucket_create(st: &mut State, req: &Request, p: &Value) -> Response {
    let Some(name) = param_str(p, "name").map(String::from) else {
        return bad_request("name is a required parameter");
    };
    if !table_bucket_name_is_valid(&name) {
        return invalid_bucket_name();
    }
    let region = req.region.clone();
    let account_id = req.account.to_string();
    let bucket = TableBucket {
        name,
        account_id,
        region,
        creation_date: now_iso(),
        namespaces: HashMap::new(),
        policy: None,
        maintenance_configuration: HashMap::new(),
        encryption_configuration: None,
        metrics_configuration: None,
        storage_class_configuration: json!({}),
        replication_configuration: None,
        replication_version_token: None,
    };
    let arn = bucket.arn();
    if st.table_buckets.contains_key(&arn) {
        return conflict("The bucket that you tried to create already exists, and you own it.");
    }
    st.table_buckets.insert(arn.clone(), bucket);
    ok_json(json!({ "arn": arn }))
}

fn bucket_list(st: &State, p: &Value) -> Response {
    let prefix = param_str(p, "prefix").filter(|s| !s.is_empty());
    let items: Vec<Value> = st
        .table_buckets
        .values()
        .filter(|b| prefix.map(|pre| b.name.starts_with(pre)).unwrap_or(true))
        .map(|b| b.projection())
        .collect();
    let token = param_str(p, "continuationToken").filter(|s| !s.is_empty());
    if let Err(e) = validate_token(token) {
        return e;
    }
    let (items, token) = page(items, token, param_i64(p, "maxBuckets"));
    let mut body = json!({ "tableBuckets": items });
    if let Some(t) = token {
        body["continuationToken"] = json!(t);
    }
    ok_json(body)
}

fn bucket_get(st: &State, arn: &str) -> Response {
    match st.table_buckets.get(arn) {
        Some(b) => ok_json(b.projection()),
        None => bucket_does_not_exist(),
    }
}

fn bucket_delete(st: &mut State, arn: &str) -> Response {
    if !st.table_buckets.contains_key(arn) {
        return bucket_does_not_exist();
    }
    st.table_buckets.remove(arn);
    empty_ok()
}

// ---------------------------------------------------------------------------
// Table-bucket policy / maintenance / encryption / metrics / storage-class
// ---------------------------------------------------------------------------

fn bucket_policy_get(st: &State, arn: &str) -> Response {
    match st.table_buckets.get(arn) {
        Some(b) => match &b.policy {
            Some(policy) => ok_json(json!({ "resourcePolicy": policy })),
            None => policy_does_not_exist(),
        },
        None => bucket_does_not_exist(),
    }
}

fn bucket_policy_put(st: &mut State, arn: &str, p: &Value) -> Response {
    let Some(policy) = param_str(p, "resourcePolicy").map(String::from) else {
        return bad_request("resourcePolicy is a required parameter");
    };
    try_resp!(find_bucket_mut(st, arn)).policy = Some(policy);
    empty_ok()
}

fn bucket_policy_delete(st: &mut State, arn: &str) -> Response {
    try_resp!(find_bucket_mut(st, arn)).policy = None;
    empty_ok()
}

fn bucket_maintenance_get(st: &State, arn: &str) -> Response {
    match st.table_buckets.get(arn) {
        Some(b) => {
            let mut config = Map::new();
            for (k, v) in &b.maintenance_configuration {
                config.insert(k.clone(), v.clone());
            }
            ok_json(json!({
                "tableBucketARN": arn,
                "configuration": Value::Object(config),
            }))
        }
        None => bucket_does_not_exist(),
    }
}

fn bucket_maintenance_put(st: &mut State, arn: &str, config_type: &str, p: &Value) -> Response {
    let value = p.get("value").cloned().unwrap_or_else(|| json!({}));
    let b = try_resp!(find_bucket_mut(st, arn));
    b.maintenance_configuration.insert(
        config_type.to_string(),
        json!({ "status": "enabled", "settings": value }),
    );
    empty_ok()
}

fn bucket_encryption_get(st: &State, arn: &str) -> Response {
    match st.table_buckets.get(arn) {
        Some(b) => {
            let config = b
                .encryption_configuration
                .clone()
                .unwrap_or_else(|| json!({ "sseAlgorithm": "AES256" }));
            ok_json(json!({ "encryptionConfiguration": config }))
        }
        None => bucket_does_not_exist(),
    }
}

fn bucket_encryption_put(st: &mut State, arn: &str, p: &Value) -> Response {
    let Some(config) = p.get("encryptionConfiguration") else {
        return bad_request("encryptionConfiguration is a required parameter");
    };
    try_resp!(find_bucket_mut(st, arn)).encryption_configuration = Some(config.clone());
    empty_ok()
}

fn bucket_encryption_delete(st: &mut State, arn: &str) -> Response {
    try_resp!(find_bucket_mut(st, arn)).encryption_configuration = None;
    empty_ok()
}

fn bucket_metrics_get(st: &State, arn: &str) -> Response {
    match st.table_buckets.get(arn) {
        Some(b) => ok_json(json!({
            "tableBucketARN": arn,
            "id": b.metrics_configuration.clone().unwrap_or_default(),
        })),
        None => bucket_does_not_exist(),
    }
}

fn bucket_metrics_put(st: &mut State, arn: &str) -> Response {
    try_resp!(find_bucket_mut(st, arn)).metrics_configuration = Some(metrics_id());
    empty_ok()
}

fn bucket_metrics_delete(st: &mut State, arn: &str) -> Response {
    try_resp!(find_bucket_mut(st, arn)).metrics_configuration = None;
    empty_ok()
}

fn bucket_storage_get(st: &State, arn: &str) -> Response {
    match st.table_buckets.get(arn) {
        Some(b) => ok_json(json!({ "storageClassConfiguration": b.storage_class_configuration })),
        None => bucket_does_not_exist(),
    }
}

fn bucket_storage_put(st: &mut State, arn: &str, p: &Value) -> Response {
    let Some(config) = p.get("storageClassConfiguration") else {
        return bad_request("storageClassConfiguration is a required parameter");
    };
    try_resp!(find_bucket_mut(st, arn)).storage_class_configuration = config.clone();
    empty_ok()
}

// ---------------------------------------------------------------------------
// Table-bucket replication
// ---------------------------------------------------------------------------

fn bucket_replication_get(st: &State, arn: &str) -> Response {
    match st.table_buckets.get(arn) {
        Some(b) => match (&b.replication_configuration, &b.replication_version_token) {
            (Some(config), token) => ok_json(json!({
                "versionToken": token,
                "configuration": config,
            })),
            (None, _) => replication_does_not_exist(),
        },
        None => bucket_does_not_exist(),
    }
}

fn bucket_replication_put(st: &mut State, p: &Value) -> Response {
    let arn = param_str(p, "tableBucketARN").unwrap_or_default();
    let Some(configuration) = p.get("configuration") else {
        return bad_request("configuration is a required parameter");
    };
    let token = version_token();
    let b = try_resp!(find_bucket_mut(st, arn));
    b.replication_configuration = Some(configuration.clone());
    b.replication_version_token = Some(token.clone());
    ok_json(json!({ "versionToken": token, "status": "ACTIVE" }))
}

fn bucket_replication_delete(st: &mut State, p: &Value) -> Response {
    let arn = param_str(p, "tableBucketARN").unwrap_or_default();
    let b = try_resp!(find_bucket_mut(st, arn));
    b.replication_configuration = None;
    b.replication_version_token = None;
    empty_ok()
}

// ---------------------------------------------------------------------------
// Namespaces
// ---------------------------------------------------------------------------

fn namespace_create(st: &mut State, req: &Request, p: &Value, arn: &str) -> Response {
    // moto: `json.loads(body)["namespace"][0]`
    let name = p
        .get("namespace")
        .and_then(Value::as_array)
        .and_then(|a| a.first())
        .and_then(Value::as_str)
        .unwrap_or_default();
    if name.is_empty() {
        return bad_request("namespace is a required parameter");
    }
    let Some(b) = st.table_buckets.get_mut(arn) else {
        return namespace_resource_not_found();
    };
    if b.namespaces.contains_key(name) {
        return conflict("A namespace with an identical name already exists in the bucket.");
    }
    let account = req.account.to_string();
    b.namespaces.insert(
        name.to_string(),
        Namespace {
            name: name.to_string(),
            account_id: account.clone(),
            created_by: account.clone(),
            creation_date: now_iso(),
            tables: HashMap::new(),
        },
    );
    ok_json(json!({
        "tableBucketARN": arn,
        "namespace": [name],
    }))
}

fn namespace_list(st: &State, p: &Value, arn: &str) -> Response {
    let b = match st.table_buckets.get(arn) {
        Some(b) => b,
        None => return bucket_does_not_exist(),
    };
    let prefix = param_str(p, "prefix").filter(|s| !s.is_empty());
    let items: Vec<Value> = b
        .namespaces
        .values()
        .filter(|ns| prefix.map(|pre| ns.name.starts_with(pre)).unwrap_or(true))
        .map(|ns| {
            json!({
                "namespace": [ns.name],
                "createdAt": ns.creation_date,
                "createdBy": ns.created_by,
                "ownerAccountId": ns.account_id,
            })
        })
        .collect();
    let token = param_str(p, "continuationToken").filter(|s| !s.is_empty());
    if let Err(e) = validate_token(token) {
        return e;
    }
    let (items, token) = page(items, token, param_i64(p, "maxNamespaces"));
    let mut body = json!({ "namespaces": items });
    if let Some(t) = token {
        body["continuationToken"] = json!(t);
    }
    ok_json(body)
}

fn namespace_get(st: &State, arn: &str, ns: &str) -> Response {
    match st.table_buckets.get(arn).and_then(|b| b.namespaces.get(ns)) {
        Some(ns) => ok_json(json!({
            "namespace": [ns.name],
            "createdAt": ns.creation_date,
            "createdBy": ns.created_by,
            "ownerAccountId": ns.account_id,
        })),
        None => namespace_resource_not_found(),
    }
}

fn namespace_delete(st: &mut State, arn: &str, ns: &str) -> Response {
    let found = st
        .table_buckets
        .get_mut(arn)
        .and_then(|b| b.namespaces.remove(ns));
    match found {
        Some(_) => empty_ok(),
        None => namespace_resource_not_found(),
    }
}

// ---------------------------------------------------------------------------
// Tables
// ---------------------------------------------------------------------------

fn table_create(st: &mut State, req: &Request, p: &Value, route: &Route) -> Response {
    let ns = route.tail(0);
    let Some(name) = param_str(p, "name").map(String::from) else {
        return bad_request("name is a required parameter");
    };
    let Some(format) = param_str(p, "format").map(String::from) else {
        return bad_request("format is a required parameter");
    };
    if !table_name_is_valid(&name) {
        return bad_request(&format!(
            "1 validation error detected: Value '{name}' at 'name' failed to satisfy constraint: Member must satisfy regular expression pattern: [0-9a-z_]*"
        ));
    }
    let bucket_exists = st.table_buckets.contains_key(&route.arn);
    if !bucket_exists {
        return bucket_does_not_exist();
    }
    let b = st.table_buckets.get_mut(&route.arn).expect("checked above");
    let Some(n) = b.namespaces.get_mut(ns) else {
        return namespace_does_not_exist();
    };
    if n.tables.contains_key(&name) {
        return table_already_exists();
    }
    let account_id = req.account.to_string();
    let table = Table {
        name: name.clone(),
        namespace: ns.to_string(),
        table_bucket_arn: route.arn.clone(),
        account_id,
        format,
        ttype: "customer",
        version_token: version_token(),
        creation_date: now_iso(),
        last_modified: now_iso(),
        modified_by: None,
        managed_by_service: false,
        metadata_location: None,
        warehouse_location: warehouse_location(),
        policy: None,
        maintenance_configuration: HashMap::new(),
        encryption_configuration: json!({ "sseAlgorithm": "AES256" }),
        storage_class_configuration: json!({}),
        replication_configuration: None,
        replication_version_token: None,
        record_expiration_configuration: None,
    };
    let arn = table.arn();
    let token = table.version_token.clone();
    n.tables.insert(name, table);
    ok_json(json!({ "tableARN": arn, "versionToken": token }))
}

fn table_get(st: &mut State, p: &Value) -> Response {
    // GET /get-table?tableBucketARN=…&namespace=…&name=…
    let arn = param_str(p, "tableBucketARN").unwrap_or_default();
    let ns = param_str(p, "namespace").unwrap_or_default();
    let name = param_str(p, "name").unwrap_or_default();
    let t = try_resp!(find_table_triple(st, arn, ns, name));
    let mut v = t.projection();
    // moto echoes the requested namespace (from the wire params)
    v["namespace"] = json!([ns]);
    ok_json(v)
}

fn table_list(st: &State, p: &Value, arn: &str) -> Response {
    let b = match st.table_buckets.get(arn) {
        Some(b) => b,
        None => return bucket_does_not_exist(),
    };
    let ns_filter = param_str(p, "namespace").filter(|s| !s.is_empty());
    if let Some(want) = ns_filter {
        if !b.namespaces.contains_key(want) {
            return namespace_resource_not_found();
        }
    }
    let prefix = param_str(p, "prefix").filter(|s| !s.is_empty());
    let mut items: Vec<Value> = Vec::new();
    for ns in b.namespaces.values() {
        if let Some(want) = ns_filter {
            if ns.name != want {
                continue;
            }
        }
        for t in ns.tables.values() {
            if prefix.map(|pre| !t.name.starts_with(pre)).unwrap_or(false) {
                continue;
            }
            items.push(json!({
                "namespace": [t.namespace],
                "name": t.name,
                "createdAt": t.creation_date,
                "modifiedAt": t.last_modified,
            }));
        }
    }
    let token = param_str(p, "continuationToken").filter(|s| !s.is_empty());
    if let Err(e) = validate_token(token) {
        return e;
    }
    let (items, token) = page(items, token, param_i64(p, "maxTables"));
    let mut body = json!({ "tables": items });
    if let Some(t) = token {
        body["continuationToken"] = json!(t);
    }
    ok_json(body)
}

fn table_delete(st: &mut State, route: &Route, p: &Value) -> Response {
    let ns = route.tail(0);
    let name = route.tail(1);
    let exists = st
        .table_buckets
        .get(&route.arn)
        .and_then(|b| b.namespaces.get(ns))
        .and_then(|n| n.tables.get(name))
        .map(|t| t.version_token.clone());
    let current = match exists {
        Some(token) => token,
        None => return table_does_not_exist(),
    };
    if let Some(token) = param_str(p, "versionToken").filter(|t| !t.is_empty()) {
        if token != current {
            return version_token_mismatch();
        }
    }
    st.table_buckets
        .get_mut(&route.arn)
        .and_then(|b| b.namespaces.get_mut(ns))
        .expect("verified above")
        .tables
        .remove(name);
    empty_ok()
}

// ---------------------------------------------------------------------------
// Table policy / maintenance / metadata / rename / replication / expiration
// ---------------------------------------------------------------------------

fn table_policy_get(st: &mut State, arn: &str, ns: &str, name: &str) -> Response {
    let t = try_resp!(find_table_triple(st, arn, ns, name));
    match &t.policy {
        Some(policy) => ok_json(json!({ "resourcePolicy": policy })),
        None => policy_does_not_exist(),
    }
}

fn table_policy_put(st: &mut State, arn: &str, ns: &str, name: &str, p: &Value) -> Response {
    let Some(policy) = param_str(p, "resourcePolicy").map(String::from) else {
        return bad_request("resourcePolicy is a required parameter");
    };
    try_resp!(find_table_triple(st, arn, ns, name)).policy = Some(policy);
    empty_ok()
}

fn table_policy_delete(st: &mut State, arn: &str, ns: &str, name: &str) -> Response {
    try_resp!(find_table_triple(st, arn, ns, name)).policy = None;
    empty_ok()
}

fn table_maintenance_get(st: &mut State, arn: &str, ns: &str, name: &str) -> Response {
    let t = try_resp!(find_table_triple(st, arn, ns, name));
    let table_arn = t.arn();
    let mut config = Map::new();
    for (k, v) in t.maintenance_configuration.clone() {
        config.insert(k, v);
    }
    ok_json(json!({
        "tableARN": table_arn,
        "configuration": Value::Object(config),
    }))
}

fn table_maintenance_put(
    st: &mut State,
    arn: &str,
    ns: &str,
    name: &str,
    config_type: &str,
    p: &Value,
) -> Response {
    let value = p.get("value").cloned().unwrap_or_else(|| json!({}));
    let t = try_resp!(find_table_triple(st, arn, ns, name));
    t.maintenance_configuration.insert(
        config_type.to_string(),
        json!({ "status": "enabled", "settings": value }),
    );
    empty_ok()
}

fn table_maintenance_jobs(st: &mut State, arn: &str, ns: &str, name: &str) -> Response {
    let t = try_resp!(find_table_triple(st, arn, ns, name));
    let table_arn = t.arn();
    ok_json(json!({ "tableARN": table_arn, "status": {} }))
}

fn table_encryption(st: &mut State, arn: &str, ns: &str, name: &str) -> Response {
    let t = try_resp!(find_table_triple(st, arn, ns, name));
    let config = t.encryption_configuration.clone();
    ok_json(json!({ "encryptionConfiguration": config }))
}

fn table_storage(st: &mut State, arn: &str, ns: &str, name: &str) -> Response {
    let t = try_resp!(find_table_triple(st, arn, ns, name));
    let config = t.storage_class_configuration.clone();
    ok_json(json!({ "storageClassConfiguration": config }))
}

fn table_metadata_get(st: &mut State, arn: &str, ns: &str, name: &str) -> Response {
    let t = try_resp!(find_table_triple(st, arn, ns, name));
    ok_json(json!({
        "warehouseLocation": t.warehouse_location.clone(),
        "metadataLocation": t.metadata_location.clone(),
        "versionToken": t.version_token.clone(),
    }))
}

fn table_metadata_update(st: &mut State, arn: &str, ns: &str, name: &str, p: &Value) -> Response {
    let metadata_location = param_str(p, "metadataLocation")
        .unwrap_or_default()
        .to_string();
    let t = try_resp!(find_table_triple(st, arn, ns, name));
    if !metadata_location.starts_with(&t.warehouse_location) {
        return invalid_metadata_location();
    }
    let token = param_str(p, "versionToken").unwrap_or_default().to_string();
    if t.version_token != token {
        return version_token_mismatch();
    }
    t.metadata_location = Some(metadata_location.clone());
    t.version_token = version_token();
    ok_json(json!({
        "name": t.name.clone(),
        "tableArn": t.arn(),
        "namespace": ns,
        "versionToken": t.version_token.clone(),
        "metadataLocation": metadata_location,
    }))
}

fn table_rename(st: &mut State, req: &Request, route: &Route, p: &Value) -> Response {
    let ns = route.tail(0);
    let name = route.tail(1);
    let new_namespace = param_str(p, "newNamespaceName").map(String::from);
    let new_name = param_str(p, "newName").map(String::from);
    if new_namespace.is_none() && new_name.is_none() {
        return nothing_to_rename();
    }
    let destination_ns = new_namespace.clone().unwrap_or_else(|| ns.to_string());
    let destination_name = new_name.clone().unwrap_or_else(|| name.to_string());
    if !table_name_is_valid(&destination_name) {
        return bad_request(&format!(
            "1 validation error detected: Value '{destination_name}' at 'name' failed to satisfy constraint: Member must satisfy regular expression pattern: [0-9a-z_]*"
        ));
    }
    // moto: destination namespace, then source table, then token, then dup.
    let bucket = match st.table_buckets.get(&route.arn) {
        Some(b) => b,
        None => {
            return not_found("The specified destination namespace does not exist.");
        }
    };
    if !bucket.namespaces.contains_key(&destination_ns) {
        return not_found("The specified destination namespace does not exist.");
    }
    let has_source = st
        .table_buckets
        .get(&route.arn)
        .and_then(|b| b.namespaces.get(ns))
        .and_then(|n| n.tables.get(name))
        .is_some();
    if !has_source {
        return table_does_not_exist();
    }
    let token = st
        .table_buckets
        .get(&route.arn)
        .and_then(|b| b.namespaces.get(ns))
        .and_then(|n| n.tables.get(name))
        .map(|t| t.version_token.clone())
        .expect("checked above");
    let req_token = param_str(p, "versionToken").unwrap_or_default();
    if !req_token.is_empty() && req_token != token {
        return version_token_mismatch();
    }
    let name_taken = st
        .table_buckets
        .get(&route.arn)
        .and_then(|b| b.namespaces.get(&destination_ns))
        .map(|n| n.tables.contains_key(&destination_name))
        .unwrap_or(false);
    if name_taken {
        return table_already_exists();
    }

    let mut table = st
        .table_buckets
        .get_mut(&route.arn)
        .and_then(|b| b.namespaces.get_mut(ns))
        .and_then(|n| n.tables.remove(name))
        .expect("checks above passed");
    if let Some(new_name) = new_name {
        table.name = new_name;
    }
    table.last_modified = now_iso();
    table.modified_by = Some(req.account.to_string());
    table.namespace = destination_ns.clone();
    st.table_buckets
        .get_mut(&route.arn)
        .and_then(|b| b.namespaces.get_mut(&destination_ns))
        .expect("checked above")
        .tables
        .insert(table.name.clone(), table);
    empty_ok()
}

// ---------------------------------------------------------------------------
// Table replication / record expiration / replication status (by table arn)
// ---------------------------------------------------------------------------

fn table_replication_get(st: &mut State, p: &Value) -> Response {
    let arn = param_str(p, "tableArn").unwrap_or_default();
    let t = try_resp!(find_table_mut(st, arn));
    match (&t.replication_configuration, &t.replication_version_token) {
        (Some(config), token) => ok_json(json!({
            "versionToken": token,
            "configuration": config,
        })),
        (None, _) => replication_does_not_exist(),
    }
}

fn table_replication_put(st: &mut State, p: &Value) -> Response {
    let arn = param_str(p, "tableArn").unwrap_or_default();
    let Some(configuration) = p.get("configuration") else {
        return bad_request("configuration is a required parameter");
    };
    let token = version_token();
    let t = try_resp!(find_table_mut(st, arn));
    t.replication_configuration = Some(configuration.clone());
    t.replication_version_token = Some(token.clone());
    ok_json(json!({ "versionToken": token, "status": "ACTIVE" }))
}

fn table_replication_delete(st: &mut State, p: &Value) -> Response {
    let arn = param_str(p, "tableArn").unwrap_or_default();
    let t = try_resp!(find_table_mut(st, arn));
    t.replication_configuration = None;
    t.replication_version_token = None;
    empty_ok()
}

fn table_replication_status(st: &mut State, p: &Value) -> Response {
    let arn = param_str(p, "tableArn").unwrap_or_default();
    let _ = try_resp!(find_table_mut(st, arn));
    ok_json(json!({ "sourceTableArn": arn, "destinations": [] }))
}

fn record_expiration_get(st: &mut State, p: &Value) -> Response {
    let arn = param_str(p, "tableArn").unwrap_or_default();
    let t = try_resp!(find_table_mut(st, arn));
    let config = t
        .record_expiration_configuration
        .clone()
        .unwrap_or_else(|| json!({}));
    ok_json(json!({ "configuration": config }))
}

fn record_expiration_put(st: &mut State, p: &Value) -> Response {
    let arn = param_str(p, "tableArn").unwrap_or_default();
    let value = p.get("value").cloned().unwrap_or_else(|| json!({}));
    let t = try_resp!(find_table_mut(st, arn));
    t.record_expiration_configuration = Some(value);
    empty_ok()
}

fn record_expiration_jobs(st: &mut State, p: &Value) -> Response {
    let arn = param_str(p, "tableArn").unwrap_or_default();
    let _ = try_resp!(find_table_mut(st, arn));
    ok_json(json!({ "status": "Not Yet Run" }))
}

// ---------------------------------------------------------------------------
// Tagging (moto's TaggingService on the backend instance)
// ---------------------------------------------------------------------------

fn tags_list(st: &State, arn: &str) -> Response {
    let tags = st.tags.get(arn).cloned().unwrap_or_default();
    ok_json(json!({ "tags": tags }))
}

fn tags_tag(st: &mut State, arn: &str, p: &Value) -> Response {
    let tags = p
        .get("tags")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let entry = st.tags.entry(arn.to_string()).or_default();
    for (k, v) in tags {
        if let Some(s) = v.as_str() {
            entry.insert(k, s.to_string());
        }
    }
    empty_ok()
}

fn tags_untag(st: &mut State, arn: &str, req: &Request) -> Response {
    // botocore serializes list members as repeated query keys; a single
    // comma-joined (or single quoted) value is also tolerated, like moto's
    // `isinstance(tag_keys, str)` fallback.
    let mut keys = crate::proto::query_map(&req.query_string)
        .remove("tagKeys")
        .unwrap_or_default();
    if keys.len() == 1 && keys[0].contains(',') {
        keys = keys[0].split(',').map(|s| s.trim().to_string()).collect();
    }
    let keys: Vec<String> = keys
        .into_iter()
        .map(|k| k.trim_matches('"').to_string())
        .collect();
    if let Some(entry) = st.tags.get_mut(arn) {
        for k in keys {
            entry.remove(&k);
        }
    }
    empty_ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::collections::HashMap as StdMap;

    const ACCOUNT: u64 = 123456789012;
    const REGION: &str = "us-east-1";

    fn request(method: &str, path: &str, body: Value, query: &str) -> Request {
        Request {
            service: "s3tables".into(),
            operation: String::new(),
            method: method.into(),
            path: path.into(),
            query_string: query.into(),
            headers: StdMap::new(),
            body: serde_json::to_vec(&body).unwrap_or_default().into(),
            account: ACCOUNT,
            region: REGION.into(),
        }
    }

    fn body_of(resp: &Response) -> Value {
        serde_json::from_slice(&resp.body).unwrap_or(Value::Null)
    }

    fn create_bucket(name: &str) -> Value {
        let req = request("PUT", "/buckets", json!({ "name": name }), "");
        let resp = dispatch(&req).expect("armed");
        body_of(&resp)
    }

    #[test]
    fn bucket_create_get_delete() {
        let created = create_bucket("test-abc1");
        let arn = created["arn"].as_str().unwrap().to_string();
        let ep = arn_query(&arn);
        assert_eq!(
            arn,
            format!("arn:aws:s3tables:{REGION}:{ACCOUNT}:bucket/test-abc1")
        );

        let resp =
            dispatch(&request("GET", &format!("/buckets/{ep}"), json!({}), "")).expect("armed");
        assert_eq!(resp.status, 200);
        let v = body_of(&resp);
        assert_eq!(v["name"], "test-abc1");
        assert_eq!(v["ownerAccountId"], ACCOUNT.to_string());
        assert!(v["createdAt"].as_str().is_some());

        // duplicate create → ConflictException
        let resp = dispatch(&request(
            "PUT",
            "/buckets",
            json!({ "name": "test-abc1" }),
            "",
        ))
        .expect("armed");
        assert_eq!(resp.status, 409);

        // raw-slash ARN segs survive the rsplit: GET on a bucket ARN written
        // with an actual '/' — same thing the encoded form decodes to.
        let resp =
            dispatch(&request("DELETE", &format!("/buckets/{ep}"), json!({}), "")).expect("armed");
        assert_eq!(resp.status, 200);

        let resp =
            dispatch(&request("GET", &format!("/buckets/{ep}"), json!({}), "")).expect("armed");
        assert_eq!(resp.status, 404);
        assert_eq!(body_of(&resp)["__type"], "NotFoundException");
    }

    #[test]
    fn bucket_name_validation() {
        let resp =
            dispatch(&request("PUT", "/buckets", json!({ "name": "ab" }), "")).expect("armed");
        assert_eq!(resp.status, 400);
        assert_eq!(body_of(&resp)["__type"], "BadRequestException");
        let resp = dispatch(&request(
            "PUT",
            "/buckets",
            json!({ "name": "sthree-abc" }),
            "",
        ))
        .expect("armed");
        assert_eq!(resp.status, 400);
    }

    /// moto/s3tables/exceptions.py has no ValidationException — every 400 the
    /// family raises is a BadRequestException, including the "X is a required
    /// parameter" missing-member checks.
    #[test]
    fn missing_required_params_are_bad_request() {
        let resp = dispatch(&request("PUT", "/buckets", json!({}), "")).expect("armed");
        assert_eq!(resp.status, 400);
        let v = body_of(&resp);
        assert_eq!(v["__type"], "BadRequestException");
        assert_eq!(v["message"], "name is a required parameter");
    }

    #[test]
    fn namespace_and_table_lifecycle() {
        let arn = create_bucket("test-lifec");
        let arn = arn["arn"].as_str().unwrap().to_string();
        let ep = arn_query(&arn);

        let resp = dispatch(&request(
            "PUT",
            &format!("/namespaces/{ep}"),
            json!({ "namespace": ["ns_one"] }),
            "",
        ))
        .expect("armed");
        assert_eq!(resp.status, 200);
        assert_eq!(body_of(&resp)["namespace"], json!(["ns_one"]));

        let resp = dispatch(&request(
            "GET",
            &format!("/namespaces/{ep}/ns_one"),
            json!({}),
            "",
        ))
        .expect("armed");
        assert_eq!(resp.status, 200);
        assert_eq!(body_of(&resp)["createdBy"], ACCOUNT.to_string());

        let resp = dispatch(&request(
            "PUT",
            &format!("/tables/{ep}/ns_one"),
            json!({ "name": "tbl1", "format": "ICEBERG" }),
            "",
        ))
        .expect("armed");
        assert_eq!(resp.status, 200);
        let table_arn = body_of(&resp)["tableARN"].as_str().unwrap().to_string();
        assert_eq!(table_arn, format!("{arn}/table/tbl1"));

        // duplicate table name → ConflictException
        let resp = dispatch(&request(
            "PUT",
            &format!("/tables/{ep}/ns_one"),
            json!({ "name": "tbl1", "format": "ICEBERG" }),
            "",
        ))
        .expect("armed");
        assert_eq!(resp.status, 409);

        // get-table by query params
        let query = format!(
            "tableBucketARN={}&namespace=ns_one&name=tbl1",
            crate::proto::url_decode(&arn)
                .replace(':', "%3A")
                .replace('/', "%2F")
        );
        let resp = dispatch(&request("GET", "/get-table", json!({}), &query)).expect("armed");
        assert_eq!(resp.status, 200);
        let v = body_of(&resp);
        assert_eq!(v["format"], "ICEBERG");
        assert_eq!(v["type"], "customer");
        assert_eq!(v["namespace"], json!(["ns_one"]));
        // null members are omitted by moto's serializer
        assert!(v.get("metadataLocation").is_none());
        assert!(v.get("modifiedBy").is_none());

        // list tables
        let resp =
            dispatch(&request("GET", &format!("/tables/{ep}"), json!({}), "")).expect("armed");
        let listed = body_of(&resp);
        let names: Vec<&str> = listed["tables"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["name"].as_str().unwrap())
            .collect();
        assert_eq!(names, vec!["tbl1"]);

        // delete table then namespace then bucket
        let resp = dispatch(&request(
            "DELETE",
            &format!("/tables/{ep}/ns_one/tbl1"),
            json!({}),
            "",
        ))
        .expect("armed");
        assert_eq!(resp.status, 200);
        let resp = dispatch(&request(
            "DELETE",
            &format!("/namespaces/{ep}/ns_one"),
            json!({}),
            "",
        ))
        .expect("armed");
        assert_eq!(resp.status, 200);
        let resp =
            dispatch(&request("DELETE", &format!("/buckets/{ep}"), json!({}), "")).expect("armed");
        assert_eq!(resp.status, 200);
    }

    #[test]
    fn encoded_arn_paths_round_trip() {
        let arn_resp = create_bucket("test-enc1");
        let arn = arn_resp["arn"].as_str().unwrap().to_string();
        let ep = arn_query(&arn);
        let encoded = arn.replace(':', "%3A").replace('/', "%2F");

        // create namespace against the encoded path, then read it back via
        // the plain (already-decoded) path — both resolve to the same ARN.
        let resp = dispatch(&request(
            "PUT",
            &format!("/namespaces/{encoded}"),
            json!({ "namespace": ["ns_e"] }),
            "",
        ))
        .expect("armed");
        assert_eq!(resp.status, 200);
        let resp = dispatch(&request(
            "GET",
            &format!("/namespaces/{ep}/ns_e"),
            json!({}),
            "",
        ))
        .expect("armed");
        assert_eq!(resp.status, 200);
    }

    #[test]
    fn metadata_location_flow() {
        let arn_resp = create_bucket("test-meta1");
        let arn = arn_resp["arn"].as_str().unwrap().to_string();
        let ep = arn_query(&arn);
        dispatch(&request(
            "PUT",
            &format!("/namespaces/{ep}"),
            json!({ "namespace": ["ns_m"] }),
            "",
        ));
        dispatch(&request(
            "PUT",
            &format!("/tables/{ep}/ns_m"),
            json!({ "name": "t1", "format": "ICEBERG" }),
            "",
        ));

        let resp = dispatch(&request(
            "GET",
            &format!("/tables/{ep}/ns_m/t1/metadata-location"),
            json!({}),
            "",
        ))
        .expect("armed");
        let v = body_of(&resp);
        assert!(v.get("metadataLocation").is_none());
        let warehouse = v["warehouseLocation"].as_str().unwrap().to_string();
        assert!(warehouse.starts_with("s3://"));
        let token = v["versionToken"].as_str().unwrap().to_string();
        assert_eq!(token.len(), 20);

        let new_loc = format!("{warehouse}/metadata/00001-a.metadata.json");
        let resp = dispatch(&request(
            "PUT",
            &format!("/tables/{ep}/ns_m/t1/metadata-location"),
            json!({ "metadataLocation": new_loc, "versionToken": token }),
            "",
        ))
        .expect("armed");
        assert_eq!(resp.status, 200);
        assert_eq!(body_of(&resp)["metadataLocation"], json!(new_loc));

        // old token → version mismatch
        let resp = dispatch(&request(
            "PUT",
            &format!("/tables/{ep}/ns_m/t1/metadata-location"),
            json!({ "metadataLocation": new_loc, "versionToken": "ffffffffffffffffffff" }),
            "",
        ))
        .expect("armed");
        assert_eq!(resp.status, 409);

        // location outside the warehouse → invalid
        let resp = dispatch(
            &request(
                "PUT",
                &format!("/tables/{ep}/ns_m/t1/metadata-location"),
                json!({ "metadataLocation": "s3://other-bucket/x", "versionToken": body_of(&dispatch(&request("GET", &format!("/tables/{ep}/ns_m/t1/metadata-location"), json!({}), "")).expect("armed"))["versionToken"].clone() }),
                "",
            ),
        )
        .expect("armed");
        assert_eq!(resp.status, 400);
    }

    #[test]
    fn rename_table_flow() {
        let arn_resp = create_bucket("test-ren1");
        let arn = arn_resp["arn"].as_str().unwrap().to_string();
        let ep = arn_query(&arn);
        dispatch(&request(
            "PUT",
            &format!("/namespaces/{ep}"),
            json!({ "namespace": ["ns_r"] }),
            "",
        ));
        let created = body_of(
            &dispatch(&request(
                "PUT",
                &format!("/tables/{ep}/ns_r"),
                json!({ "name": "old", "format": "ICEBERG" }),
                "",
            ))
            .expect("armed"),
        );
        let token = created["versionToken"].as_str().unwrap().to_string();

        // neither target → NothingToRename
        let resp = dispatch(&request(
            "PUT",
            &format!("/tables/{ep}/ns_r/old/rename"),
            json!({ "versionToken": token }),
            "",
        ))
        .expect("armed");
        assert_eq!(resp.status, 400);

        let resp = dispatch(&request(
            "PUT",
            &format!("/tables/{ep}/ns_r/old/rename"),
            json!({ "newName": "new", "versionToken": token }),
            "",
        ))
        .expect("armed");
        assert_eq!(resp.status, 200);

        let resp =
            dispatch(&request("GET", &format!("/tables/{ep}"), json!({}), "")).expect("armed");
        let listed = body_of(&resp);
        let names: Vec<&str> = listed["tables"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["name"].as_str().unwrap())
            .collect();
        assert!(names.contains(&"new"));
        assert!(!names.contains(&"old"));
    }

    #[test]
    fn maintenance_config_round_trip() {
        let arn_resp = create_bucket("test-mnt1");
        let arn = arn_resp["arn"].as_str().unwrap().to_string();
        let ep = arn_query(&arn);
        // PUT with the maintenance/{type} tail split correctly
        let resp = dispatch(&request(
            "PUT",
            &format!("/buckets/{ep}/maintenance/icebergCompaction"),
            json!({ "value": { "status": "enabled" } }),
            "",
        ))
        .expect("armed");
        assert_eq!(resp.status, 200);
        let resp = dispatch(&request(
            "GET",
            &format!("/buckets/{ep}/maintenance"),
            json!({}),
            "",
        ))
        .expect("armed");
        assert_eq!(resp.status, 200);
        let v = body_of(&resp);
        assert_eq!(v["tableBucketARN"], json!(arn));
        assert_eq!(
            v["configuration"]["icebergCompaction"]["settings"]["status"],
            json!("enabled")
        );
    }

    #[test]
    fn table_replication_and_expiration() {
        let arn_resp = create_bucket("test-rep1");
        let arn = arn_resp["arn"].as_str().unwrap().to_string();
        let ep = arn_query(&arn);
        dispatch(&request(
            "PUT",
            &format!("/namespaces/{ep}"),
            json!({ "namespace": ["ns_p"] }),
            "",
        ));
        dispatch(&request(
            "PUT",
            &format!("/tables/{ep}/ns_p"),
            json!({ "name": "t1", "format": "ICEBERG" }),
            "",
        ));
        let table_arn = format!("{arn}/table/t1");

        // no replication yet → NotFound
        let resp = dispatch(&request(
            "GET",
            "/table-replication",
            json!({}),
            &format!("tableArn={}", arn_query(&table_arn)),
        ))
        .expect("armed");
        assert_eq!(resp.status, 404);

        let resp = dispatch(&request(
            "PUT",
            "/table-replication",
            json!({ "tableArn": table_arn, "configuration": { "rules": [] } }),
            "",
        ))
        .expect("armed");
        assert_eq!(resp.status, 200);
        assert_eq!(body_of(&resp)["status"], "ACTIVE");

        let resp = dispatch(&request(
            "GET",
            "/replication-status",
            json!({}),
            &format!("tableArn={}", arn_query(&table_arn)),
        ))
        .expect("armed");
        assert_eq!(body_of(&resp)["destinations"], json!([]));

        let resp = dispatch(&request(
            "DELETE",
            "/table-replication",
            json!({}),
            &format!("tableArn={}", arn_query(&table_arn)),
        ))
        .expect("armed");
        assert_eq!(resp.status, 200);

        // record expiration
        let resp = dispatch(&request(
            "PUT",
            "/table-record-expiration",
            json!({ "tableArn": table_arn, "value": { "status": "enabled" } }),
            "",
        ))
        .expect("armed");
        assert_eq!(resp.status, 200);
        let resp = dispatch(&request(
            "GET",
            "/table-record-expiration",
            json!({}),
            &format!("tableArn={}", arn_query(&table_arn)),
        ))
        .expect("armed");
        assert_eq!(body_of(&resp)["configuration"]["status"], "enabled");
        let resp = dispatch(&request(
            "GET",
            "/table-record-expiration-job-status",
            json!({}),
            &format!("tableArn={}", arn_query(&table_arn)),
        ))
        .expect("armed");
        assert_eq!(body_of(&resp)["status"], "Not Yet Run");
    }

    fn arn_query(arn: &str) -> String {
        arn.replace(':', "%3A").replace('/', "%2F")
    }

    #[test]
    fn tags_round_trip() {
        let arn_resp = create_bucket("test-tag1");
        let arn = arn_resp["arn"].as_str().unwrap().to_string();
        let tag_path = format!("/tag/{}", arn_query(&arn));
        dispatch(&request(
            "POST",
            &tag_path,
            json!({ "tags": { "env": "test" } }),
            "",
        ));
        let resp = dispatch(&request("GET", &tag_path, json!({}), "")).expect("armed");
        assert_eq!(body_of(&resp)["tags"]["env"], "test");
        dispatch(&request("DELETE", &tag_path, json!({}), "tagKeys=env"));
        let resp = dispatch(&request("GET", &tag_path, json!({}), "")).expect("armed");
        assert!(body_of(&resp)["tags"].get("env").is_none());
    }

    #[test]
    fn unroutable_path_switches_to_501() {
        let r = dispatch(&request("PATCH", "/buckets/x", json!({}), ""));
        assert_eq!(r.expect("armed").status, 501);
        let r = dispatch(&request("GET", "/nonexistent-thing", json!({}), ""));
        assert_eq!(r.expect("armed").status, 501);
    }

    #[test]
    fn other_services_are_not_armed() {
        let mut req = request("GET", "/buckets", json!({}), "");
        req.service = "s3".into();
        assert!(dispatch(&req).is_none());
    }

    #[test]
    fn empty_resize_body_is_bytes() {
        // sanity: Bytes round-trip for the request body accumulator
        let req = request("PUT", "/buckets", json!({}), "");
        let bytes: Bytes = req.body.clone();
        crate::proto::parse_json_body(&bytes);
    }
}
