//! AWS CloudHSM v2 backend — moto-equivalent (json protocol).
//!
//! Replaces the moto sidecar for `cloudhsmv2` (`X-Amz-Target: BaldrApiService.<Op>`
//! on `application/x-amz-json-1.1`). Ground truth: the moto fork checkout's
//! `moto/cloudhsmv2` (models.py + responses.py; there is no probe file for
//! this service — the fork source is the surface) and
//! tests/compatibility/test_cloudhsmv2_compat.py. Every operation in the
//! botocore model has a routed fork handler, and all 18 are implemented here.
//!
//! Behavioral quirks kept from moto:
//! - `CreateCluster` also fabricates the initial backup; `DeleteCluster` does
//!   not (the create-time backup is what the backup filters then find);
//! - tags live in a map keyed by resource id, separate from the `TagList`
//!   carried on cluster/backup JSON: `ListTags` for an untagged (even
//!   nonexistent) resource answers an empty list, never a fault;
//! - `DeleteHsm` deletes the first HSM when no identifier is given and faults
//!   when a provided identifier matches nothing;
//! - `BackupRetentionPolicy` defaults to `{}` on create and never drops off
//!   the wire; never-initialized nullable members (`HsmType`, `NeverExpires`
//!   when `ModifyBackupAttributes` omits it) render as `null`;
//! - timestamps render like moto's `utcnow().isoformat()` (naive, no offset);
//! - `DescribeClusters` sorts oldest-first, `DescribeBackups` newest-first
//!   (unless `SortAscending`), both stable on insertion order;
//! - pagination follows moto's paginator with `fail_on_invalid_token=False`:
//!   an unusable token silently restarts at page one, a token pointing past
//!   the dataset pages nothing.

use crate::proto::{json_error, parse_json_body};
use crate::store::{self, ScopeKey};
use crate::{Request, Response};
use base64::Engine as _;
use parking_lot::RwLock;
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

type Stores = Arc<RwLock<HashMap<ScopeKey, CloudHsmStore>>>;

fn stores() -> Stores {
    store::slot::<CloudHsmStore>("cloudhsmv2")
}

/// Per-(account, region) in-memory state for AWS CloudHSM v2. Nothing is
/// pre-seeded (moto's `CloudHSMV2Backend.__init__` starts empty).
#[derive(Default)]
pub struct CloudHsmStore {
    /// Insertion-ordered (moto iterates dicts keyed by generated ids).
    clusters: Vec<Cluster>,
    backups: Vec<Backup>,
    /// resource id -> tag list — moto's `tags` map; only `TagResource` appends.
    tags: BTreeMap<String, Vec<Value>>,
    /// resource arn -> policy document (moto's `resource_policies`).
    resource_policies: BTreeMap<String, Value>,
}

fn with_store(req: &Request, f: impl FnOnce(&mut CloudHsmStore) -> Response) -> Response {
    let slot = stores();
    let mut map = slot.write();
    let st = map.entry((req.account, req.region.clone())).or_default();
    f(st)
}

// ---------------------------------------------------------------------------
// Request helpers
// ---------------------------------------------------------------------------

/// Present and non-null, mirroring moto's `x is not None` checks.
fn present<'a>(body: &'a Value, key: &str) -> Option<&'a Value> {
    body.get(key).filter(|v| !v.is_null())
}

fn as_str<'a>(body: &'a Value, key: &str) -> &'a str {
    body.get(key).and_then(Value::as_str).unwrap_or("")
}

fn as_str_list(value: Option<&Value>) -> Vec<String> {
    value
        .and_then(Value::as_array)
        .map(|list| {
            list.iter()
                .filter_map(Value::as_str)
                .map(String::from)
                .collect()
        })
        .unwrap_or_default()
}

fn as_tag_list(value: Option<&Value>) -> Vec<Value> {
    match value {
        Some(Value::Array(list)) => list.clone(),
        _ => Vec::new(),
    }
}

/// moto's `utcnow().isoformat()` — naive UTC microseconds, no offset suffix.
fn iso_now() -> String {
    chrono::Utc::now()
        .format("%Y-%m-%dT%H:%M:%S%.6f")
        .to_string()
}

fn ok(body: Value) -> Response {
    Response::json(200, serde_json::to_vec(&body).unwrap_or_default())
}

fn fault(code: &str, message: &str) -> Response {
    json_error(400, code, message)
}

fn cluster_not_found(cluster_id: &str) -> Response {
    fault(
        "ResourceNotFoundException",
        &format!("Cluster {cluster_id} not found"),
    )
}

fn backup_not_found(backup_id: &str) -> Response {
    fault(
        "ResourceNotFoundException",
        &format!("Backup {backup_id} not found"),
    )
}

// ---------------------------------------------------------------------------
// Records (wire shapes mirror moto's to_dict methods)
// ---------------------------------------------------------------------------

/// moto's `Hsm` — note `EniIp` is the (defaulted) ip address itself.
#[derive(Clone)]
struct Hsm {
    hsm_id: String,
    cluster_id: String,
    availability_zone: String,
    ip_address: String,
    hsm_type: Value,
    subnet_id: String,
    eni_id: String,
}

impl Hsm {
    /// moto's `Hsm.__init__`: `ip_address or "10.0.0.1"`.
    fn new(
        cluster_id: &str,
        availability_zone: &str,
        ip_address: &str,
        hsm_type: Value,
        subnet_id: &str,
    ) -> Hsm {
        let ip = if ip_address.is_empty() {
            "10.0.0.1"
        } else {
            ip_address
        };
        // moto truncates hyphenated uuid4 strings; keep the slice lengths.
        Hsm {
            hsm_id: format!("hsm-{:.17}", uuid::Uuid::new_v4()),
            cluster_id: cluster_id.to_string(),
            availability_zone: availability_zone.to_string(),
            ip_address: ip.to_string(),
            hsm_type: hsm_type.clone(),
            subnet_id: subnet_id.to_string(),
            eni_id: format!("eni-{:.8}", uuid::Uuid::new_v4()),
        }
    }

    fn to_dict(&self) -> Value {
        json!({
            "AvailabilityZone": self.availability_zone,
            "ClusterId": self.cluster_id,
            "SubnetId": self.subnet_id,
            "EniId": self.eni_id,
            "EniIp": self.ip_address,
            "HsmId": self.hsm_id,
            "HsmType": self.hsm_type,
            "State": "ACTIVE",
            "StateMessage": "HSM created.",
        })
    }
}

/// moto's `Cluster`. The `TagList` field is rendering-only (see module docs).
#[derive(Clone)]
struct Cluster {
    cluster_id: String,
    /// moto stores `{'Type': .., 'Value': ..}` from the create/modify call,
    /// empty dict when create omitted it.
    backup_retention_policy: Value,
    create_timestamp: String,
    hsms: Vec<Hsm>,
    hsm_type: Value,
    source_backup_id: Option<String>,
    state: String,
    state_message: String,
    region: String,
    subnet_ids: Vec<String>,
    vpc_id: String,
    network_type: String,
    /// Only `ClusterCertificate` ever changes (InitializeCluster).
    cluster_certificate: Value,
    tag_list: Vec<Value>,
    mode: String,
}

impl Cluster {
    fn new(region: &str, body: &Value) -> Cluster {
        Cluster {
            // moto: str(uuid.uuid4()) — plain uuid, no prefix.
            cluster_id: uuid::Uuid::new_v4().to_string(),
            backup_retention_policy: body
                .get("BackupRetentionPolicy")
                .cloned()
                .unwrap_or_else(|| json!({})),
            create_timestamp: iso_now(),
            hsms: Vec::new(),
            hsm_type: body.get("HsmType").cloned().unwrap_or(Value::Null),
            source_backup_id: present(body, "SourceBackupId")
                .and_then(Value::as_str)
                .map(String::from),
            state: "ACTIVE".to_string(),
            state_message: "The cluster is ready for use.".to_string(),
            region: region.to_string(),
            subnet_ids: as_str_list(body.get("SubnetIds")),
            vpc_id: format!("vpc-{:.8}", uuid::Uuid::new_v4()),
            network_type: match as_str(body, "NetworkType") {
                "" => "IPV4".to_string(),
                other => other.to_string(),
            },
            cluster_certificate: Value::String(String::new()),
            tag_list: as_tag_list(body.get("TagList")),
            mode: match as_str(body, "Mode") {
                "" => "DEFAULT".to_string(),
                other => other.to_string(),
            },
        }
    }

    /// moto's subnet mapping: `{region: subnet_id}` per subnet id — one key,
    /// so only the last id survives (moto's XXX comment acknowledges the bug).
    fn subnet_mapping(&self) -> Value {
        match self.subnet_ids.last() {
            Some(id) => json!({ self.region.clone(): id }),
            None => json!({}),
        }
    }

    fn to_dict(&self) -> Value {
        json!({
            "BackupPolicy": "DEFAULT",
            "BackupRetentionPolicy": self.backup_retention_policy,
            "ClusterId": self.cluster_id,
            "CreateTimestamp": self.create_timestamp,
            "Hsms": self.hsms.iter().map(Hsm::to_dict).collect::<Vec<_>>(),
            "HsmType": self.hsm_type,
            "SourceBackupId": self
                .source_backup_id
                .clone()
                .map(Value::from)
                .unwrap_or(Value::Null),
            "State": self.state,
            "StateMessage": self.state_message,
            "SubnetMapping": self.subnet_mapping(),
            "VpcId": self.vpc_id,
            "NetworkType": self.network_type,
            "Certificates": json!({
                "ClusterCsr": "",
                "HsmCertificate": "",
                "AwsHardwareCertificate": "",
                "ManufacturerHardwareCertificate": "",
                "ClusterCertificate": self.cluster_certificate,
            }),
            "TagList": self.tag_list,
            "Mode": self.mode,
        })
    }
}

/// moto's `Backup`.
#[derive(Clone)]
struct Backup {
    backup_id: String,
    backup_arn: String,
    backup_state: String,
    cluster_id: String,
    create_timestamp: String,
    copy_timestamp: Option<String>,
    never_expires: Value,
    source_region: Option<String>,
    source_backup: Option<String>,
    source_cluster: Option<String>,
    delete_timestamp: Option<String>,
    tag_list: Vec<Value>,
    hsm_type: Value,
    mode: String,
}

impl Backup {
    fn new(account: u64, region: &str, cluster: &Cluster) -> Backup {
        let backup_id = uuid::Uuid::new_v4().to_string();
        Backup {
            backup_arn: format!("arn:aws:cloudhsm:{region}:{account}:backup/{backup_id}"),
            backup_id,
            backup_state: "READY".to_string(),
            cluster_id: cluster.cluster_id.clone(),
            create_timestamp: iso_now(),
            copy_timestamp: None,
            never_expires: json!(false),
            source_region: None,
            source_backup: None,
            source_cluster: None,
            delete_timestamp: None,
            tag_list: cluster.tag_list.clone(),
            hsm_type: cluster.hsm_type.clone(),
            mode: cluster.mode.clone(),
        }
    }

    fn to_dict(&self) -> Value {
        let mut out = json!({
            "BackupId": self.backup_id,
            "BackupArn": self.backup_arn,
            "BackupState": self.backup_state,
            "ClusterId": self.cluster_id,
            "CreateTimestamp": self.create_timestamp,
            "NeverExpires": self.never_expires,
            "TagList": self.tag_list,
            "HsmType": self.hsm_type,
            "Mode": self.mode,
        });
        // moto emits the copy/source/delete members only when set.
        if let Some(ts) = &self.copy_timestamp {
            out["CopyTimestamp"] = json!(ts);
        }
        if let Some(r) = &self.source_region {
            out["SourceRegion"] = json!(r);
        }
        if let Some(b) = &self.source_backup {
            out["SourceBackup"] = json!(b);
        }
        if let Some(c) = &self.source_cluster {
            out["SourceCluster"] = json!(c);
        }
        if let Some(ts) = &self.delete_timestamp {
            out["DeleteTimestamp"] = json!(ts);
        }
        out
    }
}

// ---------------------------------------------------------------------------
// Pagination (moto's utilities/paginator.py, fail_on_invalid_token=False)
// ---------------------------------------------------------------------------

/// moto's `TokenEncoder`: the token payload is base64-encoded JSON.
fn encode_token(unique_attributes: &str) -> String {
    let doc = json!({"parameterChecksum": 0, "uniqueAttributes": unique_attributes});
    base64::engine::general_purpose::STANDARD.encode(serde_json::to_vec(&doc).unwrap_or_default())
}

/// Decode a next-token back to its unique-attribute value; `None` when the
/// token is unusable (moto then silently restarts at page one).
fn decode_token(token: &str) -> Option<String> {
    let raw = base64::engine::general_purpose::STANDARD
        .decode(token)
        .ok()?;
    let doc: Value = serde_json::from_slice(&raw).ok()?;
    doc.get("uniqueAttributes")?.as_str().map(String::from)
}

/// Page `items` with moto's paginator semantics (limit default 100,
/// `MaxResults` of 0 has no effect, token encodes the next page's first id).
fn paginate<T: Clone>(
    items: Vec<T>,
    unique_attr: impl Fn(&T) -> String,
    max_results: Option<&Value>,
    next_token: Option<&str>,
) -> (Vec<T>, Option<String>) {
    // moto: `max_results if max_results else limit_default` — 0 falls back.
    let limit = match max_results.and_then(Value::as_i64) {
        Some(n) if n > 0 => n as usize,
        _ => 100,
    };
    let start = match next_token {
        None => 0,
        Some(tok) => match decode_token(tok) {
            // Unusable token: full first page, no fault.
            None => 0,
            Some(attrs) => match items.iter().position(|it| unique_attr(it) == attrs) {
                Some(i) => i,
                // Token past the dataset: nothing left to page.
                None => return (Vec::new(), None),
            },
        },
    };
    let end = (start + limit).min(items.len());
    let page = items[start..end].to_vec();
    let token = if !page.is_empty() && end < items.len() {
        Some(encode_token(&unique_attr(&items[end])))
    } else {
        None
    };
    (page, token)
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "cloudhsmv2" {
        return None;
    }
    Some(with_store(req, |st| route(req, st)))
}

fn route(req: &Request, st: &mut CloudHsmStore) -> Response {
    let body = parse_json_body(&req.body);
    let account = req.account;
    let region = req.region.as_str();
    match req.operation.as_str() {
        "CopyBackupToRegion" => copy_backup_to_region(st, region, &body),
        "CreateCluster" => create_cluster(st, account, region, &body),
        "CreateHsm" => create_hsm(st, &body),
        "DeleteBackup" => delete_backup(st, &body),
        "DeleteCluster" => delete_cluster(st, &body),
        "DeleteHsm" => delete_hsm(st, &body),
        "DeleteResourcePolicy" => delete_resource_policy(st, &body),
        "DescribeBackups" => describe_backups(st, &body),
        "DescribeClusters" => describe_clusters(st, &body),
        "GetResourcePolicy" => get_resource_policy(st, &body),
        "InitializeCluster" => initialize_cluster(st, &body),
        "ListTags" => list_tags(st, &body),
        "ModifyBackupAttributes" => modify_backup_attributes(st, &body),
        "ModifyCluster" => modify_cluster(st, &body),
        "PutResourcePolicy" => put_resource_policy(st, &body),
        "RestoreBackup" => restore_backup(st, &body),
        "TagResource" => tag_resource(st, &body),
        "UntagResource" => untag_resource(st, &body),
        // Everything else is outside the moto fork's routed surface.
        other => json_error(
            501,
            "NotImplemented",
            &format!("The {other} action has not been implemented"),
        ),
    }
}

// ---------------------------------------------------------------------------
// Cluster operations
// ---------------------------------------------------------------------------

fn create_cluster(st: &mut CloudHsmStore, account: u64, region: &str, body: &Value) -> Response {
    let cluster = Cluster::new(region, body);
    let backup = Backup::new(account, region, &cluster);
    st.clusters.push(cluster.clone());
    st.backups.push(backup);
    ok(json!({ "Cluster": cluster.to_dict() }))
}

fn delete_cluster(st: &mut CloudHsmStore, body: &Value) -> Response {
    let cluster_id = as_str(body, "ClusterId");
    let Some(idx) = st.clusters.iter().position(|c| c.cluster_id == cluster_id) else {
        return cluster_not_found(cluster_id);
    };
    let mut cluster = st.clusters.remove(idx);
    cluster.state = "DELETED".to_string();
    cluster.state_message = "Cluster deleted".to_string();
    ok(json!({ "Cluster": cluster.to_dict() }))
}

fn describe_clusters(st: &CloudHsmStore, body: &Value) -> Response {
    let filters = filters_from(body);
    let mut clusters: Vec<&Cluster> = st
        .clusters
        .iter()
        .filter(|c| {
            filters.iter().all(|(key, values)| match key.as_str() {
                "clusterIds" => values.contains(&c.cluster_id),
                "states" => values.contains(&c.state),
                "vpcIds" => values.contains(&c.vpc_id),
                _ => true,
            })
        })
        .collect();
    clusters.sort_by(|a, b| a.create_timestamp.cmp(&b.create_timestamp));
    let (page, token) = paginate(
        clusters,
        |c| c.cluster_id.clone(),
        body.get("MaxResults"),
        body["NextToken"].as_str(),
    );
    ok(json!({
        "Clusters": page.iter().map(|c| c.to_dict()).collect::<Vec<_>>(),
        "NextToken": token,
    }))
}

/// Filters from the request body; boto3 sends them as a JSON object, other
/// SDKs send them JSON-encoded — moto's response layer accepts both.
fn filters_from(body: &Value) -> BTreeMap<String, Vec<String>> {
    match body.get("Filters") {
        Some(Value::String(raw)) => serde_json::from_str::<Value>(raw)
            .ok()
            .and_then(|parsed| filters_from_map(&parsed))
            .unwrap_or_default(),
        Some(Value::Object(_)) => body
            .get("Filters")
            .and_then(filters_from_map)
            .unwrap_or_default(),
        _ => BTreeMap::new(),
    }
}

fn filters_from_map(filters: &Value) -> Option<BTreeMap<String, Vec<String>>> {
    let map = filters.as_object()?;
    Some(
        map.iter()
            .filter_map(|(k, v)| {
                let list = v.as_array()?;
                Some((
                    k.clone(),
                    list.iter()
                        .filter_map(Value::as_str)
                        .map(String::from)
                        .collect(),
                ))
            })
            .collect(),
    )
}

fn create_hsm(st: &mut CloudHsmStore, body: &Value) -> Response {
    let cluster_id = as_str(body, "ClusterId");
    let Some(cluster) = st.clusters.iter_mut().find(|c| c.cluster_id == cluster_id) else {
        return cluster_not_found(cluster_id);
    };
    // moto: `subnet_id = next(iter(cluster.subnet_mapping.values()), "")` — the
    // mapping collapses to the LAST subnet id (single region key).
    let subnet_id = cluster.subnet_ids.last().cloned().unwrap_or_default();
    let hsm = Hsm::new(
        cluster_id,
        as_str(body, "AvailabilityZone"),
        as_str(body, "IpAddress"),
        cluster.hsm_type.clone(),
        &subnet_id,
    );
    cluster.hsms.push(hsm.clone());
    ok(json!({ "Hsm": hsm.to_dict() }))
}

fn delete_hsm(st: &mut CloudHsmStore, body: &Value) -> Response {
    let cluster_id = as_str(body, "ClusterId");
    let Some(idx) = st.clusters.iter().position(|c| c.cluster_id == cluster_id) else {
        return cluster_not_found(cluster_id);
    };
    let hsm_id = as_str(body, "HsmId");
    let eni_id = as_str(body, "EniId");
    let eni_ip = as_str(body, "EniIp");
    let cluster = &mut st.clusters[idx];
    let mut matched = cluster.hsms.iter().position(|h| {
        (!hsm_id.is_empty() && h.hsm_id == hsm_id)
            || (!eni_id.is_empty() && h.eni_id == eni_id)
            || (!eni_ip.is_empty() && h.ip_address == eni_ip)
    });
    // No identifier at all: moto falls back to deleting the first HSM.
    if matched.is_none()
        && hsm_id.is_empty()
        && eni_id.is_empty()
        && eni_ip.is_empty()
        && !cluster.hsms.is_empty()
    {
        matched = Some(0);
    }
    let Some(hsm_idx) = matched else {
        return fault("ResourceNotFoundException", "HSM not found");
    };
    let hsm = cluster.hsms.remove(hsm_idx);
    ok(json!({ "HsmId": hsm.hsm_id }))
}

fn modify_cluster(st: &mut CloudHsmStore, body: &Value) -> Response {
    let cluster_id = as_str(body, "ClusterId");
    let Some(cluster) = st.clusters.iter_mut().find(|c| c.cluster_id == cluster_id) else {
        return cluster_not_found(cluster_id);
    };
    // moto applies each member only when it is not None (explicit null is ignored).
    if let Some(policy) = present(body, "BackupRetentionPolicy") {
        cluster.backup_retention_policy = policy.clone();
    }
    if let Some(hsm_type) = present(body, "HsmType") {
        cluster.hsm_type = hsm_type.clone();
    }
    ok(json!({ "Cluster": cluster.to_dict() }))
}

fn initialize_cluster(st: &mut CloudHsmStore, body: &Value) -> Response {
    let cluster_id = as_str(body, "ClusterId");
    let Some(cluster) = st.clusters.iter_mut().find(|c| c.cluster_id == cluster_id) else {
        return cluster_not_found(cluster_id);
    };
    cluster.state = "INITIALIZE_IN_PROGRESS".to_string();
    cluster.state_message =
        "Cluster is initializing. State will change to INITIALIZED upon completion.".to_string();
    // moto stores SignedCert verbatim — even None.
    cluster.cluster_certificate = body.get("SignedCert").cloned().unwrap_or(Value::Null);
    ok(json!({
        "State": cluster.state,
        "StateMessage": cluster.state_message,
    }))
}

// ---------------------------------------------------------------------------
// Backup operations
// ---------------------------------------------------------------------------

fn delete_backup(st: &mut CloudHsmStore, body: &Value) -> Response {
    let backup_id = as_str(body, "BackupId");
    let Some(idx) = st.backups.iter().position(|b| b.backup_id == backup_id) else {
        return backup_not_found(backup_id);
    };
    let backup = &mut st.backups[idx];
    backup.backup_state = "PENDING_DELETION".to_string();
    backup.delete_timestamp = Some(iso_now());
    ok(json!({ "Backup": backup.to_dict() }))
}

fn restore_backup(st: &mut CloudHsmStore, body: &Value) -> Response {
    let backup_id = as_str(body, "BackupId");
    let Some(idx) = st.backups.iter().position(|b| b.backup_id == backup_id) else {
        return backup_not_found(backup_id);
    };
    let backup = &mut st.backups[idx];
    backup.backup_state = "READY".to_string();
    backup.delete_timestamp = None;
    ok(json!({ "Backup": backup.to_dict() }))
}

fn copy_backup_to_region(st: &CloudHsmStore, region: &str, body: &Value) -> Response {
    let backup_id = as_str(body, "BackupId");
    let Some(backup) = st.backups.iter().find(|b| b.backup_id == backup_id) else {
        return backup_not_found(backup_id);
    };
    ok(json!({
        "DestinationBackup": {
            "CreateTimestamp": iso_now(),
            "SourceRegion": region,
            "SourceBackup": backup_id,
            "SourceCluster": backup.cluster_id,
        }
    }))
}

fn modify_backup_attributes(st: &mut CloudHsmStore, body: &Value) -> Response {
    let backup_id = as_str(body, "BackupId");
    let Some(idx) = st.backups.iter().position(|b| b.backup_id == backup_id) else {
        return backup_not_found(backup_id);
    };
    let backup = &mut st.backups[idx];
    // moto assigns the param verbatim: absent -> None stays None.
    backup.never_expires = body.get("NeverExpires").cloned().unwrap_or(Value::Null);
    ok(json!({ "Backup": backup.to_dict() }))
}

fn describe_backups(st: &CloudHsmStore, body: &Value) -> Response {
    let filters = filters_from(body);
    let mut backups: Vec<&Backup> = st
        .backups
        .iter()
        .filter(|b| {
            filters.iter().all(|(key, values)| match key.as_str() {
                "backupIds" => values.contains(&b.backup_id),
                "sourceBackupIds" => b
                    .source_backup
                    .as_ref()
                    .map(|s| values.contains(s))
                    .unwrap_or(false),
                "clusterIds" => values.contains(&b.cluster_id),
                "states" => values.contains(&b.backup_state),
                // moto: `values[0].lower() == "true"`.
                "neverExpires" => {
                    b.never_expires.as_bool() == values.first().map(|v| v.to_lowercase() == "true")
                }
                _ => true,
            })
        })
        .collect();
    // `reverse=not sort_ascending` when given, `reverse=True` by default —
    // newest first, stable on insertion order for equal timestamps.
    match body.get("SortAscending").and_then(Value::as_bool) {
        Some(true) => backups.sort_by(|a, b| a.create_timestamp.cmp(&b.create_timestamp)),
        _ => backups.sort_by(|a, b| b.create_timestamp.cmp(&a.create_timestamp)),
    }
    let (page, token) = paginate(
        backups,
        |b| b.backup_id.clone(),
        body.get("MaxResults"),
        body["NextToken"].as_str(),
    );
    ok(json!({
        "Backups": page.iter().map(|b| b.to_dict()).collect::<Vec<_>>(),
        "NextToken": token,
    }))
}

// ---------------------------------------------------------------------------
// Resource policies
// ---------------------------------------------------------------------------

fn get_resource_policy(st: &CloudHsmStore, body: &Value) -> Response {
    let resource_arn = body.get("ResourceArn").cloned().unwrap_or(Value::Null);
    let arn = resource_arn.as_str().unwrap_or("");
    ok(json!({
        "Policy": st.resource_policies.get(arn).cloned().unwrap_or(Value::Null),
    }))
}

fn put_resource_policy(st: &mut CloudHsmStore, body: &Value) -> Response {
    let resource_arn = body.get("ResourceArn").cloned().unwrap_or(Value::Null);
    let policy = body.get("Policy").cloned().unwrap_or(Value::Null);
    // moto indexes on whatever it got — an absent Arn keys a null-ish entry.
    let key = resource_arn.as_str().unwrap_or("").to_string();
    st.resource_policies.insert(key, policy.clone());
    ok(json!({ "ResourceArn": resource_arn, "Policy": policy }))
}

fn delete_resource_policy(st: &mut CloudHsmStore, body: &Value) -> Response {
    let resource_arn = body.get("ResourceArn").cloned().unwrap_or(Value::Null);
    let key = resource_arn.as_str().unwrap_or("").to_string();
    let popped = st.resource_policies.remove(&key).unwrap_or(Value::Null);
    // moto answers only the truthy members.
    let mut result = serde_json::Map::new();
    if matches!(resource_arn.as_str(), Some(s) if !s.is_empty()) {
        result.insert("ResourceArn".into(), resource_arn);
    }
    if matches!(popped.as_str(), Some(s) if !s.is_empty()) {
        result.insert("Policy".into(), popped);
    }
    ok(Value::Object(result))
}

// ---------------------------------------------------------------------------
// Tags (moto's per-resource-id map; ListTags never faults)
// ---------------------------------------------------------------------------

fn list_tags(st: &CloudHsmStore, body: &Value) -> Response {
    let resource_id = as_str(body, "ResourceId");
    // moto returns the tag list sorted by Key; NextToken is always null
    // ("Pagination is not yet implemented").
    let mut tags: Vec<Value> = st.tags.get(resource_id).cloned().unwrap_or_default();
    tags.sort_by(|a, b| {
        a.get("Key")
            .and_then(Value::as_str)
            .unwrap_or("")
            .cmp(b.get("Key").and_then(Value::as_str).unwrap_or(""))
    });
    ok(json!({ "TagList": tags, "NextToken": Value::Null }))
}

fn tag_resource(st: &mut CloudHsmStore, body: &Value) -> Response {
    let resource_id = as_str(body, "ResourceId").to_string();
    let tags = st.tags.entry(resource_id).or_default();
    for tag in as_tag_list(body.get("TagList")) {
        let key = tag.get("Key").and_then(Value::as_str).unwrap_or("");
        match tags
            .iter_mut()
            .find(|t| t.get("Key").and_then(Value::as_str) == Some(key))
        {
            // An existing key is updated in place (position preserved).
            Some(existing) => {
                existing["Value"] = tag.get("Value").cloned().unwrap_or(Value::Null);
            }
            None => tags.push(tag),
        }
    }
    ok(json!({}))
}

fn untag_resource(st: &mut CloudHsmStore, body: &Value) -> Response {
    let resource_id = as_str(body, "ResourceId").to_string();
    let keys = as_str_list(body.get("TagKeyList"));
    if let Some(list) = st.tags.get_mut(&resource_id) {
        list.retain(|t| {
            let key = t.get("Key").and_then(Value::as_str).unwrap_or("");
            !keys.contains(&key.to_string())
        });
    }
    ok(json!({}))
}

// ---------------------------------------------------------------------------
// Unit-level tests (mirroring the sibling modules' smoke tests)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::sync::atomic::{AtomicU64, Ordering};

    static ACCOUNTS: AtomicU64 = AtomicU64::new(300_000_000_001);

    /// Every test takes a fresh account so the shared store stays isolated.
    fn next_account() -> u64 {
        ACCOUNTS.fetch_add(1, Ordering::SeqCst)
    }

    fn send(account: u64, operation: &str, body: Value) -> (u16, Value) {
        let request = Request {
            service: "cloudhsmv2".to_string(),
            operation: operation.to_string(),
            method: "POST".to_string(),
            path: "/".to_string(),
            query_string: String::new(),
            headers: HashMap::new(),
            body: Bytes::from(serde_json::to_vec(&body).unwrap()),
            account,
            region: "us-east-1".to_string(),
        };
        let resp = dispatch(&request).expect("cloudhsmv2 dispatch");
        (
            resp.status,
            serde_json::from_slice(&resp.body).unwrap_or(json!({})),
        )
    }

    fn create_cluster(account: u64, subnet_id: &str) -> String {
        let (status, body) = send(
            account,
            "CreateCluster",
            json!({"HsmType": "hsm1.medium", "SubnetIds": [subnet_id]}),
        );
        assert_eq!(status, 200, "{body}");
        body["Cluster"]["ClusterId"].as_str().unwrap().to_string()
    }

    #[test]
    fn dispatch_ignores_other_services() {
        let request = Request {
            service: "lambda".to_string(),
            operation: "CreateCluster".to_string(),
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

    #[test]
    fn create_cluster_renders_moto_shapes() {
        let (status, body) = send(
            400_000_000_001,
            "CreateCluster",
            json!({"HsmType": "hsm1.medium", "SubnetIds": ["subnet-12345678"]}),
        );
        assert_eq!(status, 200, "{body}");
        let cluster = &body["Cluster"];
        assert_eq!(cluster["HsmType"], "hsm1.medium");
        assert_eq!(cluster["State"], "ACTIVE");
        assert_eq!(cluster["BackupPolicy"], "DEFAULT");
        assert_eq!(cluster["BackupRetentionPolicy"], json!({}));
        assert_eq!(cluster["NetworkType"], "IPV4");
        assert_eq!(cluster["Mode"], "DEFAULT");
        assert_eq!(cluster["SubnetMapping"]["us-east-1"], "subnet-12345678");
        assert!(cluster["VpcId"].as_str().unwrap().starts_with("vpc-"));
        // The create-time backup exists but is invisible in DescribeClusters.
        let (status, listed) = send(400_000_000_001, "DescribeClusters", json!({}));
        assert_eq!(status, 200, "{listed}");
        assert_eq!(listed["Clusters"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn delete_cluster_faults_on_unknown_and_renders_deleted_state() {
        let account = next_account();
        let (status, body) = send(
            account,
            "DeleteCluster",
            json!({"ClusterId": "cluster-nope"}),
        );
        assert_eq!(status, 400, "{body}");
        assert_eq!(body["__type"], "ResourceNotFoundException");

        let cluster_id = create_cluster(account, "subnet-12345678");
        let (status, body) = send(account, "DeleteCluster", json!({"ClusterId": cluster_id}));
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["Cluster"]["State"], "DELETED");
        assert_eq!(body["Cluster"]["StateMessage"], "Cluster deleted");
    }

    #[test]
    fn create_hsm_uses_last_subnet_and_defaults_ip() {
        let account = next_account();
        let cluster_id = create_cluster(account, "subnet-12345678");
        let (status, body) = send(
            account,
            "CreateHsm",
            json!({
                "ClusterId": cluster_id,
                "AvailabilityZone": "us-east-1a",
            }),
        );
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["Hsm"]["ClusterId"], cluster_id);
        assert_eq!(body["Hsm"]["SubnetId"], "subnet-12345678");
        assert_eq!(body["Hsm"]["EniIp"], "10.0.0.1");
        assert!(body["Hsm"]["HsmId"].as_str().unwrap().starts_with("hsm-"));

        // Unknown cluster → cluster fault.
        let (status, body) = send(
            account,
            "CreateHsm",
            json!({"ClusterId": "cluster-nope", "AvailabilityZone": "us-east-1a"}),
        );
        assert_eq!(status, 400, "{body}");
        assert_eq!(body["__type"], "ResourceNotFoundException");
    }

    #[test]
    fn delete_hsm_falls_back_to_first_and_faults_when_empty() {
        let account = next_account();
        let cluster_id = create_cluster(account, "subnet-12345678");
        // No HSM yet, no identifier → moto's `HSM not found` fault.
        let (status, body) = send(account, "DeleteHsm", json!({"ClusterId": cluster_id}));
        assert_eq!(status, 400, "{body}");
        assert_eq!(body["__type"], "ResourceNotFoundException");
        assert_eq!(body["message"], "HSM not found");

        let (status, created) = send(
            account,
            "CreateHsm",
            json!({
                "ClusterId": cluster_id, "AvailabilityZone": "us-east-1a"
            }),
        );
        assert_eq!(status, 200, "{created}");
        let hsm_id = created["Hsm"]["HsmId"].as_str().unwrap().to_string();
        // Unknown cluster → cluster fault first.
        let (status, body) = send(account, "DeleteHsm", json!({"ClusterId": "cluster-nope"}));
        assert_eq!(status, 400, "{body}");
        assert_eq!(body["__type"], "ResourceNotFoundException");
        // No identifier, one HSM present → the first is deleted.
        let (status, body) = send(account, "DeleteHsm", json!({"ClusterId": cluster_id}));
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["HsmId"], hsm_id);
    }

    #[test]
    fn create_time_backup_is_the_only_backup() {
        let account = next_account();
        let cluster_id = create_cluster(account, "subnet-12345678");
        // Deleting the cluster does NOT create a new backup (moto quirk).
        send(account, "DeleteCluster", json!({"ClusterId": cluster_id}));
        let (status, body) = send(
            account,
            "DescribeBackups",
            json!({"Filters": {"clusterIds": [cluster_id]}}),
        );
        assert_eq!(status, 200, "{body}");
        let backups = body["Backups"].as_array().unwrap();
        assert_eq!(backups.len(), 1, "{backups:?}");
        assert_eq!(backups[0]["ClusterId"], cluster_id);
        assert_eq!(backups[0]["BackupState"], "READY");
        assert!(backups[0]["BackupArn"]
            .as_str()
            .unwrap()
            .starts_with("arn:aws:cloudhsm:us-east-1:"));
    }

    #[test]
    fn delete_backup_then_restore_roundtrip() {
        let account = next_account();
        let cluster_id = create_cluster(account, "subnet-12345678");
        let (status, listed) = send(
            account,
            "DescribeBackups",
            json!({"Filters": {"clusterIds": [cluster_id]}}),
        );
        assert_eq!(status, 200, "{listed}");
        let backup_id = listed["Backups"][0]["BackupId"]
            .as_str()
            .unwrap()
            .to_string();

        let (status, body) = send(account, "DeleteBackup", json!({"BackupId": backup_id}));
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["Backup"]["BackupState"], "PENDING_DELETION");
        assert_eq!(body["Backup"]["BackupId"], backup_id);

        let (status, body) = send(account, "RestoreBackup", json!({"BackupId": backup_id}));
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["Backup"]["BackupState"], "READY");
    }

    #[test]
    fn modify_backup_attributes_sets_never_expires() {
        let account = next_account();
        let cluster_id = create_cluster(account, "subnet-12345678");
        let (_, listed) = send(
            account,
            "DescribeBackups",
            json!({"Filters": {"clusterIds": [cluster_id]}}),
        );
        let backup_id = listed["Backups"][0]["BackupId"]
            .as_str()
            .unwrap()
            .to_string();
        let (status, body) = send(
            account,
            "ModifyBackupAttributes",
            json!({"BackupId": backup_id, "NeverExpires": true}),
        );
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["Backup"]["NeverExpires"], json!(true));
    }

    #[test]
    fn copy_backup_to_region_reports_sources() {
        let account = next_account();
        let cluster_id = create_cluster(account, "subnet-12345678");
        let (_, listed) = send(
            account,
            "DescribeBackups",
            json!({"Filters": {"clusterIds": [cluster_id]}}),
        );
        let backup_id = listed["Backups"][0]["BackupId"]
            .as_str()
            .unwrap()
            .to_string();
        let (status, body) = send(
            account,
            "CopyBackupToRegion",
            json!({
                "BackupId": backup_id, "DestinationRegion": "us-west-2"
            }),
        );
        assert_eq!(status, 200, "{body}");
        let dest = &body["DestinationBackup"];
        assert_eq!(dest["SourceBackup"], backup_id);
        assert_eq!(dest["SourceCluster"], cluster_id);
        assert_eq!(dest["SourceRegion"], "us-east-1");

        let (status, body) = send(
            account,
            "CopyBackupToRegion",
            json!({"BackupId": "backup-nope", "DestinationRegion": "us-west-2"}),
        );
        assert_eq!(status, 400, "{body}");
        assert_eq!(body["__type"], "ResourceNotFoundException");
    }

    #[test]
    fn resource_policies_roundtrip() {
        let account = next_account();
        let arn = "arn:aws:cloudhsm:us-east-1:123456789012:cluster/c1";
        let (status, body) = send(
            account,
            "PutResourcePolicy",
            json!({"ResourceArn": arn, "Policy": "{}"}),
        );
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["Policy"], "{}");

        let (status, body) = send(account, "GetResourcePolicy", json!({"ResourceArn": arn}));
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["Policy"], "{}");

        // Unknown ARN answers Policy: null (moto never faults here).
        let (status, body) = send(
            account,
            "GetResourcePolicy",
            json!({"ResourceArn": "arn:aws:cloudhsm:us-east-1:1:cluster/none"}),
        );
        assert_eq!(status, 200, "{body}");

        let (status, body) = send(account, "DeleteResourcePolicy", json!({"ResourceArn": arn}));
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["ResourceArn"], arn);
        assert_eq!(body["Policy"], "{}");
        let (status, body) = send(account, "GetResourcePolicy", json!({"ResourceArn": arn}));
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["Policy"], Value::Null);
    }

    #[test]
    fn tags_roundtrip_and_list_sorts_by_key() {
        let account = next_account();
        let cluster_id = create_cluster(account, "subnet-12345678");
        let (status, body) = send(
            account,
            "TagResource",
            json!({
                "ResourceId": cluster_id,
                "TagList": [
                    {"Key": "Zed", "Value": "1"},
                    {"Key": "Env", "Value": "test"},
                ],
            }),
        );
        assert_eq!(status, 200, "{body}");
        let (status, body) = send(account, "ListTags", json!({"ResourceId": cluster_id}));
        assert_eq!(status, 200, "{body}");
        let keys: Vec<&str> = body["TagList"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["Key"].as_str().unwrap())
            .collect();
        assert_eq!(keys, vec!["Env", "Zed"]); // sorted by Key

        // ListTags for an unknown resource answers an empty list, no fault.
        let (status, body) = send(
            account,
            "ListTags",
            json!({"ResourceId": "cluster-doesnotexist"}),
        );
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["TagList"], json!([]));

        send(
            account,
            "UntagResource",
            json!({
                "ResourceId": cluster_id, "TagKeyList": ["Env"],
            }),
        );
        let (_status, body) = send(account, "ListTags", json!({"ResourceId": cluster_id}));
        let keys: Vec<&str> = body["TagList"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["Key"].as_str().unwrap())
            .collect();
        assert_eq!(keys, vec!["Zed"]);
    }

    #[test]
    fn initialize_cluster_sets_state_and_certificate() {
        let account = next_account();
        let cluster_id = create_cluster(account, "subnet-12345678");
        let (status, body) = send(
            account,
            "InitializeCluster",
            json!({
                "ClusterId": cluster_id,
                "SignedCert": "CERT",
                "TrustAnchor": "ANCHOR",
            }),
        );
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["State"], "INITIALIZE_IN_PROGRESS");

        let (_, listed) = send(
            account,
            "DescribeClusters",
            json!({"Filters": {"clusterIds": [cluster_id]}}),
        );
        assert_eq!(listed["Clusters"][0]["State"], "INITIALIZE_IN_PROGRESS");
        assert_eq!(
            listed["Clusters"][0]["Certificates"]["ClusterCertificate"],
            "CERT"
        );

        let (status, body) = send(
            account,
            "InitializeCluster",
            json!({
                "ClusterId": "cluster-nope",
            }),
        );
        assert_eq!(status, 400, "{body}");
        assert_eq!(body["__type"], "ResourceNotFoundException");
    }

    #[test]
    fn modify_cluster_updates_policy_and_filters_match_states() {
        let account = next_account();
        let cluster_id = create_cluster(account, "subnet-12345678");
        let (status, body) = send(
            account,
            "ModifyCluster",
            json!({
                "ClusterId": cluster_id,
                "BackupRetentionPolicy": {"Type": "DAYS", "Value": "7"},
            }),
        );
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["Cluster"]["BackupRetentionPolicy"]["Value"], "7");

        let (_, listed) = send(
            account,
            "DescribeClusters",
            json!({"Filters": {"clusterIds": [cluster_id], "states": ["DELETED"]}}),
        );
        assert!(listed["Clusters"].as_array().unwrap().is_empty());

        let (_, listed) = send(
            account,
            "DescribeClusters",
            json!({"Filters": {"clusterIds": [cluster_id], "states": ["ACTIVE"]}}),
        );
        assert_eq!(listed["Clusters"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn describe_results_paginate_with_next_token() {
        let account = next_account();
        create_cluster(account, "subnet-12345678");
        create_cluster(account, "subnet-12345679");
        let (status, page) = send(account, "DescribeClusters", json!({"MaxResults": 1}));
        assert_eq!(status, 200, "{page}");
        assert_eq!(page["Clusters"].as_array().unwrap().len(), 1);
        let token = page["NextToken"].as_str().expect("next token");
        let (status, page2) = send(
            account,
            "DescribeClusters",
            json!({
                "MaxResults": 1, "NextToken": token
            }),
        );
        assert_eq!(status, 200, "{page2}");
        assert_eq!(page2["Clusters"].as_array().unwrap().len(), 1);
        assert!(page2["NextToken"].is_null());
        // An unusable token restarts at page one (fail_on_invalid_token=False).
        let (status, page3) = send(
            account,
            "DescribeClusters",
            json!({
                "MaxResults": 1, "NextToken": "not a token"
            }),
        );
        assert_eq!(status, 200, "{page3}");
        assert_eq!(page3["Clusters"].as_array().unwrap().len(), 1);
        assert!(!page3["NextToken"].is_null());
    }

    #[test]
    fn operations_outside_moto_fork_answer_501() {
        let (status, body) = send(next_account(), "NopeOperation", json!({}));
        assert_eq!(status, 501, "{body}");
        assert_eq!(body["__type"], "NotImplemented");
    }
}
