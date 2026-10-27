//! AWS MemoryDB backend — moto-equivalent (json protocol).
//!
//! Replaces the moto sidecar for `memorydb` (`X-Amz-Target: AmazonMemoryDB.<Op>`
//! on `application/x-amz-json-1.1`). State is per-(account, region), mirroring
//! moto's `MemoryDBBackend` (moto/memorydb): clusters, snapshots, subnet
//! groups, ACLs, users, parameter groups, and the moto `TaggingService` tag
//! map keyed by ARN.
//!
//! Contract: probes/memorydb.json (per-operation moto-era behavior) and
//! tests/compatibility/test_memorydb_compat.py. Behavioral quirks kept from
//! the moto fork:
//! - falsy members (None / "" / 0 / {} / []) drop off the wire, except
//!   `TLSEnabled`, `AutoMinorVersionUpgrade`, and `SnapshotRetentionLimit`
//!   which are always emitted (`MemoryDBCluster.to_dict`);
//! - unrecognized engine versions are rewritten to `7.1` / `7.1.1`;
//! - `UpdateCluster` stores the new fields, but the rendered `SecurityGroups`
//!   list stays the one frozen at creation (moto's `update` never refreshes
//!   it), and updated engine versions bypass the patch-version mapping;
//! - TLS-off clusters switch to the `open-access` ACL; users default to
//!   `on ~* +@all` and `no-password` authentication;
//! - the `default` subnet group is seeded from the EC2 backend's
//!   default-for-az subnets (the EC2 state lazily provisions the default VPC);
//! - Tag* APIs validate the ARN against cluster/snapshot/subnet-group ARNs
//!   only and pick the fault by the ARN's resource-type substring.
//!
//! Operations outside the moto fork's routed surface (BatchUpdateCluster,
//! CopySnapshot, DescribeParameters, the multi-region family, ...) answer the
//! generic 501, exactly as the probe recorded for the sidecar era.

use crate::proto::{json_error, parse_json_body};
use crate::store::{self, ScopeKey};
use crate::{Request, Response};
use parking_lot::RwLock;
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use crate::services::ec2;

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

type Stores = Arc<RwLock<HashMap<ScopeKey, MemoryDbStore>>>;

fn stores() -> Stores {
    store::slot::<MemoryDbStore>("memorydb")
}

/// Per-(account, region) in-memory state for AWS MemoryDB.
pub struct MemoryDbStore {
    /// cluster name -> cluster
    clusters: BTreeMap<String, Cluster>,
    /// snapshot name -> snapshot
    snapshots: BTreeMap<String, Snapshot>,
    /// subnet group name -> subnet group (moto pre-seeds `default`)
    subnet_groups: BTreeMap<String, SubnetGroup>,
    /// acl name -> acl (moto pre-seeds `open-access`)
    acls: BTreeMap<String, Acl>,
    /// user name -> user (moto pre-seeds `default`)
    users: BTreeMap<String, User>,
    /// parameter group name -> parameter group
    parameter_groups: BTreeMap<String, ParameterGroup>,
    /// arn -> {tag key: value} — moto's TaggingService
    tags: BTreeMap<String, BTreeMap<String, Option<String>>>,
}

/// Unseeded default: every scope reaching the backend goes through
/// [`with_store`], which builds the pre-seeded [`MemoryDbStore::new`].
impl Default for MemoryDbStore {
    fn default() -> Self {
        MemoryDbStore {
            clusters: BTreeMap::new(),
            snapshots: BTreeMap::new(),
            subnet_groups: BTreeMap::new(),
            acls: BTreeMap::new(),
            users: BTreeMap::new(),
            parameter_groups: BTreeMap::new(),
            tags: BTreeMap::new(),
        }
    }
}

impl MemoryDbStore {
    /// moto's `MemoryDBBackend.__init__`: the `open-access` ACL, the `default`
    /// user, the `default.memorydb-redis7` parameter group, and the `default`
    /// subnet group built from the EC2 backend's default subnets.
    fn new(account: u64, region: &str) -> Self {
        let mut st = MemoryDbStore::default();
        st.acls.insert(
            "open-access".to_string(),
            Acl::new(account, region, "open-access", Vec::new()),
        );
        st.users.insert(
            "default".to_string(),
            User::new(account, region, "default", "on ~* +@all", None),
        );
        st.parameter_groups.insert(
            "default.memorydb-redis7".to_string(),
            ParameterGroup::new(
                account,
                region,
                "default.memorydb-redis7",
                "memorydb_redis7",
                "Default parameter group for memorydb-redis7",
            ),
        );
        st.subnet_groups.insert(
            "default".to_string(),
            SubnetGroup {
                name: "default".to_string(),
                description: "Default MemoryDB Subnet Group".to_string(),
                subnet_ids: ec2_default_subnet_ids(account, region),
                arn: format!("arn:aws:memorydb:{region}:{account}:subnetgroup/default"),
            },
        );
        st
    }

    /// moto's `_list_arns`: every arn the Tag* APIs accept.
    fn list_arns(&self) -> Vec<String> {
        let mut arns: Vec<String> = self.clusters.values().map(|c| c.arn.clone()).collect();
        arns.extend(self.snapshots.values().map(|s| s.arn.clone()));
        arns.extend(self.subnet_groups.values().map(|s| s.arn.clone()));
        arns
    }
}

fn with_store(req: &Request, f: impl FnOnce(&mut MemoryDbStore) -> Response) -> Response {
    let slot = stores();
    let mut map = slot.write();
    let st = map
        .entry((req.account, req.region.clone()))
        .or_insert_with(|| MemoryDbStore::new(req.account, &req.region));
    f(st)
}

// ---------------------------------------------------------------------------
// EC2 subnet bridge (moto's `from moto.ec2 import ec2_backends` dependency)
// ---------------------------------------------------------------------------

/// A live-resolved EC2 subnet, mirroring moto's subnet-id describe probe.
struct Ec2Subnet {
    id: String,
    az: String,
    vpc_id: String,
}

/// Resolve `subnet_ids` against the EC2 state for the scope, dropping ids that
/// don't exist (a subnet-id filter returns only existing subnets).
fn ec2_subnets(account: u64, region: &str, subnet_ids: &[String]) -> Vec<Ec2Subnet> {
    let key: ScopeKey = (account, region.to_string());
    let slot = ec2::state::shared();
    let guard = slot.read();
    let Some(st) = guard.get(&key) else {
        return Vec::new();
    };
    st.subnets
        .iter()
        .filter(|s| subnet_ids.contains(&s.id))
        .map(|s| Ec2Subnet {
            id: s.id.clone(),
            az: s.az.clone(),
            vpc_id: s.vpc_id.clone(),
        })
        .collect()
}

/// Default-for-az subnet ids; the EC2 state provisions the default VPC when a
/// scope is first touched (moto's ec2 backend does this at construction).
fn ec2_default_subnet_ids(account: u64, region: &str) -> Vec<String> {
    let key: ScopeKey = (account, region.to_string());
    let slot = ec2::state::shared();
    let mut map = slot.write();
    let st = map.entry(key).or_insert_with(|| {
        let mut s = ec2::state::Ec2State::default();
        s.ensure_default_vpc(region, account);
        s
    });
    st.subnets
        .iter()
        .filter(|s| s.is_default)
        .map(|s| s.id.clone())
        .collect()
}

// ---------------------------------------------------------------------------
// Records (wire shapes mirror moto's to_dict methods, falsy-dropping included)
// ---------------------------------------------------------------------------

/// Python falsiness, as moto's dict filters use it.
fn python_falsy(v: &Value) -> bool {
    match v {
        Value::Null => true,
        Value::Bool(b) => !*b,
        Value::Number(n) => n.as_f64() == Some(0.0),
        Value::String(s) => s.is_empty(),
        Value::Array(a) => a.is_empty(),
        Value::Object(o) => o.is_empty(),
    }
}

fn is_truthy(v: &Value) -> bool {
    !python_falsy(v)
}

/// Remove JSON fields whose python truthiness is false (moto's
/// `{k: v for k, v in dct.items() if v}`).
fn drop_falsy(value: &mut Value) {
    if let Some(map) = value.as_object_mut() {
        let drop_keys: Vec<String> = map
            .iter()
            .filter(|(_, v)| python_falsy(v))
            .map(|(k, _)| k.clone())
            .collect();
        for key in drop_keys {
            map.remove(&key);
        }
    }
}

#[derive(Clone)]
struct Cluster {
    name: String,
    node_type: String,
    /// `default.memorydb-redis7` when the request omitted it.
    parameter_group_name: String,
    description: String,
    num_shards: i64,
    num_replicas_per_shard: i64,
    subnet_group_name: String,
    vpc_id: String,
    maintenance_window: String,
    port: i64,
    sns_topic_arn: String,
    tls_enabled: bool,
    acl_name: String,
    kms_key_id: String,
    snapshot_retention_limit: i64,
    snapshot_window: String,
    /// Rendered `"SecurityGroups"` — frozen at creation (moto's `update` never
    /// refreshes it although it stores the new ids).
    security_groups: Vec<Value>,
    engine_version: String,
    engine_patch_version: String,
    auto_minor_version_upgrade: bool,
    /// moto stores the `"true"`/`"false"` strings.
    data_tiering: String,
    status: String,
    availability_mode: String,
    arn: String,
    sns_topic_status: String,
    region: String,
}

impl Cluster {
    fn new(account: u64, region: &str, body: &Value, vpc_id: &str) -> Cluster {
        let name = as_str(body, "ClusterName").to_string();
        let tls = match body.get("TLSEnabled") {
            Some(Value::Bool(b)) => *b,
            // An absent or null TLS flag defaults to enabled.
            _ => true,
        };
        // moto maps the two known majors to their patch releases; anything
        // else — including an absent value — becomes 7.1 / 7.1.1.
        let (engine_version, engine_patch_version) = match as_str(body, "EngineVersion") {
            "7.0" => ("7.0".to_string(), "7.0.7".to_string()),
            "6.2" => ("6.2".to_string(), "6.2.6".to_string()),
            _ => ("7.1".to_string(), "7.1.1".to_string()),
        };
        Cluster {
            name: name.clone(),
            node_type: as_str(body, "NodeType").to_string(),
            parameter_group_name: match as_str(body, "ParameterGroupName") {
                "" => "default.memorydb-redis7".to_string(),
                other => other.to_string(),
            },
            description: as_str(body, "Description").to_string(),
            num_shards: match body.get("NumShards").and_then(Value::as_i64) {
                Some(0) | None => 1,
                Some(n) => n,
            },
            num_replicas_per_shard: match body.get("NumReplicasPerShard").and_then(Value::as_i64) {
                Some(0) | None => 1,
                Some(n) => n,
            },
            subnet_group_name: match as_str(body, "SubnetGroupName") {
                "" => "default".to_string(),
                other => other.to_string(),
            },
            vpc_id: vpc_id.to_string(),
            maintenance_window: match as_str(body, "MaintenanceWindow") {
                "" => "wed:08:00-wed:09:00".to_string(),
                other => other.to_string(),
            },
            port: match body.get("Port").and_then(Value::as_i64) {
                Some(0) | None => 6379,
                Some(p) => p,
            },
            sns_topic_arn: as_str(body, "SnsTopicArn").to_string(),
            tls_enabled: tls,
            // TLS-off clusters must use the open-access ACL for open auth.
            acl_name: if !tls {
                "open-access".to_string()
            } else {
                as_str(body, "ACLName").to_string()
            },
            kms_key_id: as_str(body, "KmsKeyId").to_string(),
            snapshot_retention_limit: body
                .get("SnapshotRetentionLimit")
                .and_then(Value::as_i64)
                .unwrap_or(0),
            snapshot_window: match as_str(body, "SnapshotWindow") {
                "" => "03:00-04:00".to_string(),
                other => other.to_string(),
            },
            security_groups: body
                .get("SecurityGroupIds")
                .and_then(Value::as_array)
                .map(|ids| {
                    ids.iter()
                        .filter_map(Value::as_str)
                        .map(|id| json!({"SecurityGroupId": id, "Status": "active"}))
                        .collect()
                })
                .unwrap_or_default(),
            engine_version,
            engine_patch_version,
            auto_minor_version_upgrade: match body.get("AutoMinorVersionUpgrade") {
                Some(Value::Bool(b)) => *b,
                _ => true,
            },
            data_tiering: if body
                .get("DataTiering")
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                "true".to_string()
            } else {
                "false".to_string()
            },
            status: "available".to_string(),
            availability_mode: "MultiAZ".to_string(),
            arn: format!("arn:aws:memorydb:{region}:{account}:cluster/{name}"),
            sns_topic_status: if as_str(body, "SnsTopicArn").is_empty() {
                String::new()
            } else {
                "active".to_string()
            },
            region: region.to_string(),
        }
    }

    /// moto's `MemoryDBCluster.to_dict()` — truthy members only, with three
    /// fields forced back on afterwards.
    fn to_dict(&self) -> Value {
        let region = self.region.as_str();
        let mut out = json!({
            "Name": self.name,
            "Description": self.description,
            "Status": self.status,
            "NumberOfShards": self.num_shards,
            "AvailabilityMode": self.availability_mode,
            "ClusterEndpoint": {
                "Address": format!(
                    "clustercfg.{}.aoneci.memorydb.{region}.amazonaws.com",
                    self.name
                ),
                "Port": self.port,
            },
            "NodeType": self.node_type,
            "EngineVersion": self.engine_version,
            "EnginePatchVersion": self.engine_patch_version,
            "ParameterGroupName": self.parameter_group_name,
            "ParameterGroupStatus": "in-sync",
            "SecurityGroups": self.security_groups,
            "SubnetGroupName": self.subnet_group_name,
            "KmsKeyId": self.kms_key_id,
            "ARN": self.arn,
            "SnsTopicArn": self.sns_topic_arn,
            "SnsTopicStatus": self.sns_topic_status,
            "MaintenanceWindow": self.maintenance_window,
            "SnapshotWindow": self.snapshot_window,
            "ACLName": self.acl_name,
            "DataTiering": self.data_tiering,
        });
        drop_falsy(&mut out);
        out["TLSEnabled"] = json!(self.tls_enabled);
        out["AutoMinorVersionUpgrade"] = json!(self.auto_minor_version_upgrade);
        out["SnapshotRetentionLimit"] = json!(self.snapshot_retention_limit);
        out
    }

    /// `to_desc_dict` — adds live shard details (moto stores them at creation
    /// and regenerates on shard-config updates; the values are random).
    fn to_desc_dict(&self) -> Value {
        let mut out = self.to_dict();
        out["Shards"] = json!(shard_details(self));
        out
    }
}

/// moto's `MemoryDBCluster.get_shard_details()`.
fn shard_details(cluster: &Cluster) -> Vec<Value> {
    let region = cluster.region.as_str();
    let mut shards = Vec::new();
    for i in 0..cluster.num_shards {
        let shard_name = format!("{:04}", i + 1);
        let num_nodes = cluster.num_replicas_per_shard + 1;
        let hex = uuid::Uuid::new_v4().simple().to_string();
        let digits = hex.as_bytes();
        // moto's Slots: "0-{randint(10000, 99999)}".
        let slots = 10_000
            + (digits[0] % 90) as u32 * 1_000
            + (digits[1] % 10) as u32 * 10
            + (digits[2] % 10) as u32;
        let azs = ["a", "b", "c", "d"];
        let nodes: Vec<Value> = (0..num_nodes)
            .map(|n| {
                let node_name = format!("{}-{}-{:03}", cluster.name, shard_name, n + 1);
                let az_letter = azs[(digits[(n as usize + 3) % 4] % 4) as usize];
                json!({
                    "Name": node_name,
                    "Status": "available",
                    "AvailabilityZone": format!("{region}{az_letter}"),
                    "CreateTime": crate::proto::now_iso(),
                    "Endpoint": {
                        "Address": format!(
                            "{node_name}.{}.aoneci.memorydb.{region}.amazonaws.com",
                            cluster.name
                        ),
                        "Port": cluster.port,
                    },
                })
            })
            .collect();
        shards.push(json!({
            "Name": shard_name,
            "Status": "available",
            "Slots": format!("0-{slots}"),
            "Nodes": nodes,
            "NumberOfNodes": num_nodes,
        }));
    }
    shards
}

struct Snapshot {
    cluster_name: String,
    name: String,
    status: String,
    source: String,
    kms_key_id: String,
    arn: String,
    /// Frozen `ClusterConfiguration` (no `Shards` — detail views add those).
    cluster_config: Value,
    /// moto's snapshot shard shapes, rendered under `Shards` on detail views.
    shards: Vec<Value>,
}

impl Snapshot {
    /// moto's `MemoryDBSnapshot.__init__` — a frozen copy of the cluster.
    fn from_cluster(
        account: u64,
        region: &str,
        cluster: &Cluster,
        name: &str,
        kms_key_id: &str,
        source: &str,
    ) -> Snapshot {
        Snapshot {
            cluster_name: cluster.name.clone(),
            name: name.to_string(),
            status: "available".to_string(),
            source: source.to_string(),
            kms_key_id: if kms_key_id.is_empty() {
                cluster.kms_key_id.clone()
            } else {
                kms_key_id.to_string()
            },
            arn: format!("arn:aws:memorydb:{region}:{account}:snapshot/{name}"),
            cluster_config: json!({
                "Name": cluster.name,
                "Description": cluster.description,
                "NodeType": cluster.node_type,
                "EngineVersion": cluster.engine_version,
                "MaintenanceWindow": cluster.maintenance_window,
                "TopicArn": cluster.sns_topic_arn,
                "Port": cluster.port,
                "ParameterGroupName": cluster.parameter_group_name,
                "SubnetGroupName": cluster.subnet_group_name,
                "VpcId": cluster.vpc_id,
                "SnapshotRetentionLimit": cluster.snapshot_retention_limit,
                "SnapshotWindow": cluster.snapshot_window,
                "NumShards": cluster.num_shards,
            }),
            shards: shard_details(cluster)
                .iter()
                .map(|shard| {
                    json!({
                        "Name": shard["Name"],
                        "Configuration": {
                            "Slots": shard["Slots"],
                            "ReplicaCount": cluster.num_replicas_per_shard,
                        },
                        "Size": "11 MB",
                        "SnapshotCreationTime": crate::proto::now_iso(),
                    })
                })
                .collect(),
        }
    }

    /// moto's `MemoryDBSnapshot.to_dict()`: only the outer dict is
    /// truthiness-filtered, so `ClusterConfiguration` keeps null members.
    fn to_dict(&self) -> Value {
        let mut out = json!({
            "Name": self.name,
            "Status": self.status,
            "Source": self.source,
            "KmsKeyId": self.kms_key_id,
            "ARN": self.arn,
            "ClusterConfiguration": self.cluster_config,
        });
        drop_falsy(&mut out);
        out
    }

    /// `to_desc_dict` — tucks the shard detail shapes inside the config.
    fn to_desc_dict(&self) -> Value {
        let mut out = self.to_dict();
        if let Some(config) = out.get_mut("ClusterConfiguration") {
            config["Shards"] = json!(self.shards.clone());
        }
        out
    }
}

struct SubnetGroup {
    name: String,
    description: String,
    subnet_ids: Vec<String>,
    arn: String,
}

impl SubnetGroup {
    /// moto resolves the stored subnet ids against the EC2 backend live.
    fn to_dict(&self, account: u64, region: &str) -> Value {
        let subnets = ec2_subnets(account, region, &self.subnet_ids);
        let subnet_list: Vec<Value> = subnets
            .iter()
            .map(|s| json!({"Identifier": s.id, "AvailabilityZone": {"Name": s.az}}))
            .collect();
        json!({
            "Name": self.name,
            "Description": self.description,
            "VpcId": subnets
                .first()
                .map(|s| s.vpc_id.clone())
                .unwrap_or_default(),
            "Subnets": subnet_list,
            "ARN": self.arn,
        })
    }
}

struct Acl {
    name: String,
    user_names: Vec<String>,
    status: String,
    arn: String,
}

impl Acl {
    fn new(account: u64, region: &str, name: &str, user_names: Vec<String>) -> Acl {
        Acl {
            name: name.to_string(),
            user_names,
            status: "active".to_string(),
            arn: format!("arn:aws:memorydb:{region}:{account}:acl/{name}"),
        }
    }

    fn to_dict(&self) -> Value {
        json!({
            "Name": self.name,
            "Status": self.status,
            "UserNames": self.user_names,
            "MinimumEngineVersion": "6.2",
            "ARN": self.arn,
            "Clusters": [],
        })
    }
}

struct User {
    name: String,
    access_string: String,
    /// Raw `AuthenticationMode` object; only its `Type` renders.
    authentication_mode: Option<Value>,
    status: String,
    arn: String,
}

impl User {
    fn new(
        account: u64,
        region: &str,
        name: &str,
        access_string: &str,
        mode: Option<&Value>,
    ) -> User {
        User {
            name: name.to_string(),
            access_string: if access_string.is_empty() {
                "on ~* +@all".to_string()
            } else {
                access_string.to_string()
            },
            authentication_mode: mode.cloned(),
            status: "active".to_string(),
            arn: format!("arn:aws:memorydb:{region}:{account}:user/{name}"),
        }
    }

    fn to_dict(&self) -> Value {
        json!({
            "Name": self.name,
            "Status": self.status,
            "AccessString": self.access_string,
            "ACLNames": [],
            "MinimumEngineVersion": "6.2",
            "Authentication": {
                "Type": self.authentication_mode
                    .as_ref()
                    .and_then(|m| m.get("Type"))
                    .and_then(Value::as_str)
                    .unwrap_or("no-password"),
            },
            "ARN": self.arn,
        })
    }
}

struct ParameterGroup {
    name: String,
    family: String,
    description: String,
    arn: String,
}

impl ParameterGroup {
    fn new(
        account: u64,
        region: &str,
        name: &str,
        family: &str,
        description: &str,
    ) -> ParameterGroup {
        ParameterGroup {
            name: name.to_string(),
            family: family.to_string(),
            description: description.to_string(),
            arn: format!("arn:aws:memorydb:{region}:{account}:parametergroup/{name}"),
        }
    }

    fn to_dict(&self) -> Value {
        json!({
            "Name": self.name,
            "Family": self.family,
            "Description": self.description,
            "ARN": self.arn,
        })
    }
}

// ---------------------------------------------------------------------------
// dispatch
// ---------------------------------------------------------------------------

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "memorydb" {
        return None;
    }
    Some(with_store(req, |st| route(req, st)))
}

fn route(req: &Request, st: &mut MemoryDbStore) -> Response {
    let body = parse_json_body(&req.body);
    let account = req.account;
    let region = req.region.as_str();
    match req.operation.as_str() {
        // --- clusters -------------------------------------------------------
        "CreateCluster" => create_cluster(st, account, region, &body),
        "DescribeClusters" => describe_clusters(st, &body),
        "UpdateCluster" => update_cluster(st, &body),
        "DeleteCluster" => delete_cluster(st, account, region, &body),
        // --- snapshots ---------------------------------------------------------
        "CreateSnapshot" => route_create_snapshot(st, account, region, &body),
        "DescribeSnapshots" => describe_snapshots(st, &body),
        "DeleteSnapshot" => match st.snapshots.remove(as_str(&body, "SnapshotName")) {
            Some(mut snap) => {
                snap.status = "deleting".to_string();
                ok(json!({ "Snapshot": snap.to_dict() }))
            }
            // moto's fault takes the name as its whole message.
            None => fault("SnapshotNotFoundFault", as_str(&body, "SnapshotName")),
        },
        // --- subnet groups --------------------------------------------------------
        "CreateSubnetGroup" => create_subnet_group(st, account, region, &body),
        "DescribeSubnetGroups" => describe_subnet_groups(st, account, region, &body),
        "DeleteSubnetGroup" => delete_subnet_group(st, account, region, &body),
        // --- ACLs ---------------------------------------------------------------
        "CreateACL" => create_acl(st, account, region, &body),
        "DescribeACLs" => {
            let acl_name = as_str(&body, "ACLName");
            let acls: Vec<Value> = if acl_name.is_empty() {
                st.acls.values().map(Acl::to_dict).collect()
            } else {
                match st.acls.get(acl_name) {
                    Some(a) => vec![a.to_dict()],
                    None => {
                        return fault("ACLNotFoundFault", &format!("ACL {acl_name} not found."))
                    }
                }
            };
            ok(json!({ "ACLs": acls }))
        }
        "DeleteACL" => match st.acls.remove(as_str(&body, "ACLName")) {
            Some(mut acl) => {
                acl.status = "deleting".to_string();
                ok(json!({ "ACL": acl.to_dict() }))
            }
            None => fault(
                "ACLNotFoundFault",
                &format!("ACL {} not found.", as_str(&body, "ACLName")),
            ),
        },
        "UpdateACL" => update_acl(st, &body),
        // --- users --------------------------------------------------------------
        "CreateUser" => create_user(st, account, region, &body),
        "DescribeUsers" => {
            let user_name = as_str(&body, "UserName");
            let users: Vec<Value> = if user_name.is_empty() {
                st.users.values().map(User::to_dict).collect()
            } else {
                match st.users.get(user_name) {
                    Some(u) => vec![u.to_dict()],
                    None => {
                        return fault("UserNotFoundFault", &format!("User {user_name} not found."))
                    }
                }
            };
            ok(json!({ "Users": users }))
        }
        "DeleteUser" => match st.users.remove(as_str(&body, "UserName")) {
            Some(mut user) => {
                user.status = "deleting".to_string();
                ok(json!({ "User": user.to_dict() }))
            }
            None => fault(
                "UserNotFoundFault",
                &format!("User {} not found.", as_str(&body, "UserName")),
            ),
        },
        "UpdateUser" => update_user(st, &body),
        // --- parameter groups -------------------------------------------------------
        "CreateParameterGroup" => create_parameter_group(st, account, region, &body),
        "DescribeParameterGroups" => describe_parameter_groups(st, &body),
        "DeleteParameterGroup" => {
            match st
                .parameter_groups
                .remove(as_str(&body, "ParameterGroupName"))
            {
                Some(pg) => ok(json!({ "ParameterGroup": pg.to_dict() })),
                None => fault(
                    "ParameterGroupNotFoundFault",
                    &format!(
                        "Parameter group {} not found.",
                        as_str(&body, "ParameterGroupName")
                    ),
                ),
            }
        }
        // moto ignores the parameter values; the group is returned unchanged.
        "UpdateParameterGroup" => {
            match st.parameter_groups.get(as_str(&body, "ParameterGroupName")) {
                Some(pg) => ok(json!({ "ParameterGroup": pg.to_dict() })),
                None => fault(
                    "ParameterGroupNotFoundFault",
                    &format!(
                        "Parameter group {} not found.",
                        as_str(&body, "ParameterGroupName")
                    ),
                ),
            }
        }
        // --- tags --------------------------------------------------------------------
        "ListTags" => {
            let arn = as_str(&body, "ResourceArn");
            if !st.list_arns().contains(&arn.to_string()) {
                return arn_not_found(arn);
            }
            ok(json!({ "TagList": list_tags_for(st, arn) }))
        }
        "TagResource" => {
            let arn = as_str(&body, "ResourceArn");
            if !st.list_arns().contains(&arn.to_string()) {
                return arn_not_found(arn);
            }
            tag_resource(st, arn, body.get("Tags").and_then(Value::as_array));
            ok(json!({ "TagList": list_tags_for(st, arn) }))
        }
        "UntagResource" => untag_resource_route(st, &body),
        // --- static describe lists ----------------------------------------------
        "DescribeServiceUpdates" => ok(json!({ "ServiceUpdates": [] })),
        "DescribeEvents" => ok(json!({ "Events": [] })),
        "DescribeEngineVersions" => ok(json!({ "EngineVersions": [
            {"EngineVersion": "7.0", "EnginePatchVersion": "7.0.7", "ParameterGroupFamily": "memorydb_redis7"},
            {"EngineVersion": "6.2", "EnginePatchVersion": "6.2.6", "ParameterGroupFamily": "memorydb_redis6"},
        ]})),
        "DescribeReservedNodes" => ok(json!({ "ReservedNodes": [] })),
        "DescribeReservedNodesOfferings" => ok(json!({ "ReservedNodesOfferings": [] })),
        // --- operations outside the moto fork's routed surface -------------------
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

fn create_cluster(st: &mut MemoryDbStore, account: u64, region: &str, body: &Value) -> Response {
    let name = as_str(body, "ClusterName");
    if st.clusters.contains_key(name) {
        return fault(
            "ClusterAlreadyExistsFault",
            "Cluster with specified name already exists.",
        );
    }
    // moto: `subnet_group_name or "default"`; an unknown group name crashes
    // moto with a KeyError — a named fault is answered instead.
    let sg_name = match as_str(body, "SubnetGroupName") {
        "" => "default".to_string(),
        other => other.to_string(),
    };
    let Some(sg) = st.subnet_groups.get(&sg_name) else {
        return fault(
            "SubnetGroupNotFoundFault",
            &format!("Subnet group {sg_name} not found."),
        );
    };
    let vpc_id = ec2_subnets(account, region, &sg.subnet_ids)
        .first()
        .map(|s| s.vpc_id.clone())
        .unwrap_or_default();
    let cluster = Cluster::new(account, region, body, &vpc_id);
    st.clusters.insert(name.to_string(), cluster);
    let arn = st.clusters[name].arn.clone();
    tag_resource(st, &arn, body.get("Tags").and_then(Value::as_array));
    ok(json!({ "Cluster": st.clusters[name].to_dict() }))
}

fn describe_clusters(st: &MemoryDbStore, body: &Value) -> Response {
    let name = as_str(body, "ClusterName");
    let show_shard_details = body.get("ShowShardDetails").is_some_and(is_truthy);
    if !name.is_empty() {
        return match st.clusters.get(name) {
            Some(c) if show_shard_details => ok(json!({ "Clusters": [c.to_desc_dict()] })),
            Some(c) => ok(json!({ "Clusters": [c.to_dict()] })),
            None => fault("ClusterNotFoundFault", &format!("Cluster {name} not found")),
        };
    }
    let clusters: Vec<Value> = st
        .clusters
        .values()
        .map(|c| {
            if show_shard_details {
                c.to_desc_dict()
            } else {
                c.to_dict()
            }
        })
        .collect();
    ok(json!({ "Clusters": clusters }))
}

fn update_cluster(st: &mut MemoryDbStore, body: &Value) -> Response {
    let name = as_str(body, "ClusterName");
    let Some(cluster) = st.clusters.get_mut(name) else {
        return fault("ClusterNotFoundFault", "Cluster not found.");
    };
    // moto's `Cluster.update`: every field applies only when present and
    // non-null. Note `SecurityGroupIds` is stored but never rendered back
    // (the wire list is frozen at creation), so it is simply ignored here.
    if let Some(v) = present(body, "Description") {
        cluster.description = field_str(v);
    }
    if let Some(v) = present(body, "MaintenanceWindow") {
        cluster.maintenance_window = field_str(v);
    }
    if let Some(v) = present(body, "SnsTopicArn") {
        cluster.sns_topic_arn = field_str(v);
        cluster.sns_topic_status = if cluster.sns_topic_arn.is_empty() {
            String::new()
        } else {
            "active".to_string()
        };
    }
    if let Some(v) = present(body, "SnsTopicStatus") {
        cluster.sns_topic_status = field_str(v);
    }
    if let Some(v) = present(body, "ParameterGroupName") {
        cluster.parameter_group_name = field_str(v);
    }
    if let Some(v) = present(body, "SnapshotWindow") {
        cluster.snapshot_window = field_str(v);
    }
    if let Some(v) = present(body, "SnapshotRetentionLimit") {
        cluster.snapshot_retention_limit = v.as_i64().unwrap_or(0);
    }
    if let Some(v) = present(body, "NodeType") {
        cluster.node_type = field_str(v);
    }
    // moto's update stores the engine version as given — no patch mapping.
    if let Some(v) = present(body, "EngineVersion") {
        cluster.engine_version = field_str(v);
    }
    // Shard-config updates refresh the stored shard geometry (rendered only
    // with ShowShardDetails, which regenerates from it).
    if let Some(cfg) = present(body, "ReplicaConfiguration") {
        cluster.num_replicas_per_shard =
            cfg.get("ReplicaCount").and_then(Value::as_i64).unwrap_or(0);
    }
    if let Some(cfg) = present(body, "ShardConfiguration") {
        cluster.num_shards = cfg.get("ShardCount").and_then(Value::as_i64).unwrap_or(0);
    }
    if let Some(v) = present(body, "ACLName") {
        cluster.acl_name = field_str(v);
    }
    ok(json!({ "Cluster": cluster.to_dict() }))
}

fn delete_cluster(st: &mut MemoryDbStore, account: u64, region: &str, body: &Value) -> Response {
    let name = as_str(body, "ClusterName");
    let Some(mut cluster) = st.clusters.get(name).cloned() else {
        return fault("ClusterNotFoundFault", name);
    };
    cluster.status = "deleting".to_string();
    // moto creates the final snapshot while the cluster still exists, and
    // pops the cluster only afterwards.
    if let Some(final_snapshot) = present(body, "FinalSnapshotName").and_then(Value::as_str) {
        if let Err(resp) = create_snapshot(
            st,
            account,
            region,
            name,
            final_snapshot,
            "",
            None,
            "manual",
        ) {
            return resp;
        }
    }
    st.clusters.remove(name);
    ok(json!({ "Cluster": cluster.to_dict() }))
}

fn route_create_snapshot(
    st: &mut MemoryDbStore,
    account: u64,
    region: &str,
    body: &Value,
) -> Response {
    let cluster_name = as_str(body, "ClusterName");
    let snapshot_name = as_str(body, "SnapshotName");
    if let Err(resp) = create_snapshot(
        st,
        account,
        region,
        cluster_name,
        snapshot_name,
        as_str(body, "KmsKeyId"),
        body.get("Tags").and_then(Value::as_array),
        "manual",
    ) {
        return resp;
    }
    ok(json!({ "Snapshot": st.snapshots[snapshot_name].to_dict() }))
}

/// moto's `MemoryDBBackend.create_snapshot`: the cluster must exist, the
/// snapshot name must be free, tags stored when truthy.
#[allow(clippy::too_many_arguments)]
fn create_snapshot(
    st: &mut MemoryDbStore,
    account: u64,
    region: &str,
    cluster_name: &str,
    snapshot_name: &str,
    kms_key_id: &str,
    tags: Option<&Vec<Value>>,
    source: &str,
) -> Result<(), Response> {
    let Some(cluster) = st.clusters.get(cluster_name) else {
        return Err(fault(
            "ClusterNotFoundFault",
            &format!("Cluster not found: {cluster_name}"),
        ));
    };
    if st.snapshots.contains_key(snapshot_name) {
        return Err(fault(
            "SnapshotAlreadyExistsFault",
            "Snapshot with specified name already exists.",
        ));
    }
    let snapshot =
        Snapshot::from_cluster(account, region, cluster, snapshot_name, kms_key_id, source);
    let arn = snapshot.arn.clone();
    st.snapshots.insert(snapshot_name.to_string(), snapshot);
    if let Some(tags) = tags.filter(|t| !t.is_empty()) {
        tag_resource(st, &arn, Some(tags));
    }
    Ok(())
}

fn describe_snapshots(st: &MemoryDbStore, body: &Value) -> Response {
    let cluster_name = as_str(body, "ClusterName");
    let snapshot_name = as_str(body, "SnapshotName");
    let show_detail = body.get("ShowDetail").is_some_and(is_truthy);
    let sources: Vec<String> = match as_str(body, "Source") {
        "" => vec!["automated".to_string(), "manual".to_string()],
        s => vec![s.to_string()],
    };
    let matches_source = |snap: &Snapshot| sources.contains(&snap.source);

    if !cluster_name.is_empty() && !snapshot_name.is_empty() {
        // moto scans insertion order; the first match wins, otherwise fault.
        let found = st.snapshots.values().find(|s| {
            s.cluster_name == cluster_name && s.name == snapshot_name && matches_source(s)
        });
        return match found {
            Some(s) if show_detail => ok(json!({ "Snapshots": [s.to_desc_dict()] })),
            Some(s) => ok(json!({ "Snapshots": [s.to_dict()] })),
            None => fault(
                "SnapshotNotFoundFault",
                &format!("Snapshot with name {snapshot_name} not found"),
            ),
        };
    }
    let list: Vec<Value> = st
        .snapshots
        .values()
        .filter(|s| matches_source(s))
        .filter(|s| cluster_name.is_empty() || s.cluster_name == cluster_name)
        .filter(|s| snapshot_name.is_empty() || s.name == snapshot_name)
        .map(|s| {
            if show_detail {
                s.to_desc_dict()
            } else {
                s.to_dict()
            }
        })
        .collect();
    if !snapshot_name.is_empty() && list.is_empty() {
        return fault(
            "SnapshotNotFoundFault",
            &format!("Snapshot with name {snapshot_name} not found"),
        );
    }
    ok(json!({ "Snapshots": list }))
}

fn create_subnet_group(
    st: &mut MemoryDbStore,
    account: u64,
    region: &str,
    body: &Value,
) -> Response {
    let name = as_str(body, "SubnetGroupName");
    if st.subnet_groups.contains_key(name) {
        return fault(
            "SubnetGroupAlreadyExistsFault",
            &format!("Subnet group {name} already exists."),
        );
    }
    let subnet_ids = as_str_list(body.get("SubnetIds"));
    // moto probes the EC2 backend at construction; an empty resolution is
    // InvalidSubnetError. The raw ids are stored — descriptions resolve live.
    if ec2_subnets(account, region, &subnet_ids).is_empty() {
        let list = subnet_ids
            .iter()
            .map(|id| format!("'{id}'"))
            .collect::<Vec<_>>()
            .join(", ");
        return fault(
            "InvalidSubnetError",
            &format!("Subnet ['{list}'] not found."),
        );
    }
    let group = SubnetGroup {
        name: name.to_string(),
        description: as_str(body, "Description").to_string(),
        subnet_ids,
        arn: format!("arn:aws:memorydb:{region}:{account}:subnetgroup/{name}"),
    };
    let arn = group.arn.clone();
    st.subnet_groups.insert(name.to_string(), group);
    if let Some(tags) = body
        .get("Tags")
        .and_then(Value::as_array)
        .filter(|t| !t.is_empty())
    {
        tag_resource(st, &arn, Some(tags));
    }
    ok(json!({ "SubnetGroup": st.subnet_groups[name].to_dict(account, region) }))
}

fn describe_subnet_groups(
    st: &MemoryDbStore,
    account: u64,
    region: &str,
    body: &Value,
) -> Response {
    let name = as_str(body, "SubnetGroupName");
    let groups: Vec<Value> = if name.is_empty() {
        st.subnet_groups
            .values()
            .map(|g| g.to_dict(account, region))
            .collect()
    } else {
        match st.subnet_groups.get(name) {
            Some(g) => vec![g.to_dict(account, region)],
            None => {
                return fault(
                    "SubnetGroupNotFoundFault",
                    &format!("Subnet group {name} not found."),
                )
            }
        }
    };
    ok(json!({ "SubnetGroups": groups }))
}

fn delete_subnet_group(
    st: &mut MemoryDbStore,
    account: u64,
    region: &str,
    body: &Value,
) -> Response {
    let name = as_str(body, "SubnetGroupName");
    if !st.subnet_groups.contains_key(name) {
        return fault(
            "SubnetGroupNotFoundFault",
            &format!("Subnet group {name} not found."),
        );
    }
    if name == "default" {
        return fault(
            "InvalidParameterValueException",
            "default is reserved and cannot be modified.",
        );
    }
    if st.clusters.values().any(|c| c.subnet_group_name == name) {
        return fault(
            "SubnetGroupInUseFault",
            &format!("Subnet group {name} is currently in use by a cluster."),
        );
    }
    let group = st.subnet_groups.remove(name).expect("checked above");
    ok(json!({ "SubnetGroup": group.to_dict(account, region) }))
}

fn create_acl(st: &mut MemoryDbStore, account: u64, region: &str, body: &Value) -> Response {
    let acl_name = as_str(body, "ACLName");
    if st.acls.contains_key(acl_name) {
        return fault(
            "ACLAlreadyExistsFault",
            &format!("ACL {acl_name} already exists."),
        );
    }
    let acl = Acl::new(
        account,
        region,
        acl_name,
        as_str_list(body.get("UserNames")),
    );
    // moto tags via the tagger directly, without the ARN-membership check.
    tag_resource(st, &acl.arn, body.get("Tags").and_then(Value::as_array));
    let dict = acl.to_dict();
    st.acls.insert(acl_name.to_string(), acl);
    ok(json!({ "ACL": dict }))
}

fn update_acl(st: &mut MemoryDbStore, body: &Value) -> Response {
    let acl_name = as_str(body, "ACLName");
    let Some(acl) = st.acls.get_mut(acl_name) else {
        return fault("ACLNotFoundFault", &format!("ACL {acl_name} not found."));
    };
    for name in as_str_list(body.get("UserNamesToAdd")) {
        acl.user_names.push(name);
    }
    let remove = as_str_list(body.get("UserNamesToRemove"));
    acl.user_names.retain(|u| !remove.contains(u));
    ok(json!({ "ACL": acl.to_dict() }))
}

fn create_user(st: &mut MemoryDbStore, account: u64, region: &str, body: &Value) -> Response {
    let user_name = as_str(body, "UserName");
    if st.users.contains_key(user_name) {
        return fault(
            "UserAlreadyExistsFault",
            &format!("User {user_name} already exists."),
        );
    }
    let user = User::new(
        account,
        region,
        user_name,
        as_str(body, "AccessString"),
        body.get("AuthenticationMode"),
    );
    // moto tags via the tagger directly, without the ARN-membership check.
    tag_resource(st, &user.arn, body.get("Tags").and_then(Value::as_array));
    let dict = user.to_dict();
    st.users.insert(user_name.to_string(), user);
    ok(json!({ "User": dict }))
}

fn update_user(st: &mut MemoryDbStore, body: &Value) -> Response {
    let user_name = as_str(body, "UserName");
    let Some(user) = st.users.get_mut(user_name) else {
        return fault("UserNotFoundFault", &format!("User {user_name} not found."));
    };
    // moto's string truthiness: an empty access string keeps the old one.
    let access = as_str(body, "AccessString").to_string();
    if !access.is_empty() {
        user.access_string = access;
    }
    // moto's dict truthiness: an empty/null AuthenticationMode keeps the old.
    if let Some(mode) = present(body, "AuthenticationMode") {
        user.authentication_mode = Some(mode.clone());
    }
    ok(json!({ "User": user.to_dict() }))
}

fn create_parameter_group(
    st: &mut MemoryDbStore,
    account: u64,
    region: &str,
    body: &Value,
) -> Response {
    let name = as_str(body, "ParameterGroupName");
    if st.parameter_groups.contains_key(name) {
        return fault(
            "ParameterGroupAlreadyExistsFault",
            &format!("Parameter group {name} already exists."),
        );
    }
    let pg = ParameterGroup::new(
        account,
        region,
        name,
        match body.get("Family").and_then(Value::as_str) {
            None | Some("") => "memorydb_redis7",
            Some(f) => f,
        },
        as_str(body, "Description"),
    );
    // moto tags via the tagger directly, without the ARN-membership check.
    tag_resource(st, &pg.arn, body.get("Tags").and_then(Value::as_array));
    let dict = pg.to_dict();
    st.parameter_groups.insert(name.to_string(), pg);
    ok(json!({ "ParameterGroup": dict }))
}

fn describe_parameter_groups(st: &MemoryDbStore, body: &Value) -> Response {
    let name = as_str(body, "ParameterGroupName");
    let groups: Vec<Value> = if name.is_empty() {
        st.parameter_groups
            .values()
            .map(ParameterGroup::to_dict)
            .collect()
    } else {
        match st.parameter_groups.get(name) {
            Some(pg) => vec![pg.to_dict()],
            None => {
                return fault(
                    "ParameterGroupNotFoundFault",
                    &format!("Parameter group {name} not found."),
                )
            }
        }
    };
    ok(json!({ "ParameterGroups": groups }))
}

fn untag_resource_route(st: &mut MemoryDbStore, body: &Value) -> Response {
    let arn = as_str(body, "ResourceArn");
    if !st.list_arns().contains(&arn.to_string()) {
        return arn_not_found(arn);
    }
    let keys = as_str_list(body.get("TagKeys"));
    let current: Vec<String> = list_tags_for(st, arn)
        .iter()
        .filter_map(|t| t.get("Key").and_then(Value::as_str).map(String::from))
        .collect();
    let invalid: Vec<&String> = keys.iter().filter(|k| !current.contains(k)).collect();
    if !invalid.is_empty() {
        // moto's message: `These tags are not present : [['a', 'b']]`.
        let listed = invalid
            .iter()
            .map(|k| format!("'{k}'"))
            .collect::<Vec<_>>()
            .join(", ");
        return fault(
            "TagNotFoundFault",
            &format!("These tags are not present : [['{listed}']]"),
        );
    }
    if let Some(row) = st.tags.get_mut(arn) {
        for key in &keys {
            row.remove(key);
        }
    }
    ok(json!({ "TagList": list_tags_for(st, arn) }))
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

fn ok(body: Value) -> Response {
    Response::json(200, serde_json::to_vec(&body).unwrap_or_default())
}

fn fault(code: &str, message: &str) -> Response {
    json_error(400, code, message)
}

/// Present and non-null, mirroring moto's `if x is not None` checks.
fn present<'a>(body: &'a Value, key: &str) -> Option<&'a Value> {
    body.get(key).filter(|v| !v.is_null())
}

/// Field value as a string (already filtered to non-null by the caller).
fn field_str(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
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

/// moto's Tag* unknown-ARN mapping: the resource type segment decides the fault.
fn arn_not_found(arn: &str) -> Response {
    let resource_name = arn.split('/').next_back().unwrap_or("");
    if arn.contains("subnetgroup") {
        fault(
            "SubnetGroupNotFoundFault",
            &format!("{resource_name} is not present"),
        )
    } else if arn.contains("snapshot") {
        fault(
            "SnapshotNotFoundFault",
            &format!("{resource_name} is not present"),
        )
    } else {
        fault(
            "ClusterNotFoundFault",
            &format!("{resource_name} is not present"),
        )
    }
}

/// moto's `TaggingService.tag_resource`: absent members store null values.
fn tag_resource(st: &mut MemoryDbStore, arn: &str, tags: Option<&Vec<Value>>) {
    let Some(tags) = tags else {
        return;
    };
    let row = st.tags.entry(arn.to_string()).or_default();
    for tag in tags {
        let Some(key) = tag.get("Key").and_then(Value::as_str) else {
            continue;
        };
        row.insert(
            key.to_string(),
            tag.get("Value").and_then(Value::as_str).map(String::from),
        );
    }
}

/// Tags as `[{Key, Value}]`, mirroring moto's `TaggingService`.
fn list_tags_for(st: &MemoryDbStore, arn: &str) -> Vec<Value> {
    st.tags
        .get(arn)
        .map(|row| {
            row.iter()
                .map(|(k, v)| {
                    json!({
                        "Key": k,
                        "Value": v.as_ref().map(String::as_str).map_or(Value::Null, |s| json!(s)),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
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
            service: "memorydb".to_string(),
            operation: operation.to_string(),
            method: "POST".to_string(),
            path: "/".to_string(),
            query_string: String::new(),
            headers: HashMap::new(),
            body: Bytes::from(serde_json::to_vec(&body).unwrap()),
            account,
            region: "us-east-1".to_string(),
        };
        let resp = dispatch(&request).expect("memorydb dispatch");
        (
            resp.status,
            serde_json::from_slice(&resp.body).unwrap_or(json!({})),
        )
    }

    fn create_cluster_ok(account: u64, name: &str) {
        let (status, body) = send(
            account,
            "CreateCluster",
            json!({"ClusterName": name, "NodeType": "db.t4g.small", "ACLName": "open-access"}),
        );
        assert_eq!(status, 200, "create_cluster: {body}");
    }

    #[test]
    fn cluster_lifecycle() {
        let account = next_account();
        let (status, body) = send(
            account,
            "CreateCluster",
            json!({"ClusterName": "cl-1", "NodeType": "db.t4g.small", "ACLName": "open-access"}),
        );
        assert_eq!(status, 200, "{body}");
        let cluster = &body["Cluster"];
        assert_eq!(cluster["Name"], "cl-1");
        assert_eq!(cluster["NodeType"], "db.t4g.small");
        assert_eq!(cluster["Status"], "available");
        assert_eq!(cluster["SubnetGroupName"], "default");
        assert_eq!(cluster["ParameterGroupName"], "default.memorydb-redis7");
        assert_eq!(cluster["TLSEnabled"], json!(true));
        assert_eq!(cluster["SnapshotRetentionLimit"], json!(0));
        assert_eq!(cluster["EngineVersion"], "7.1");
        assert!(cluster["ARN"]
            .as_str()
            .unwrap()
            .starts_with(&format!("arn:aws:memorydb:us-east-1:{account}:cluster/")));

        // UpdateCluster applies only the described change.
        let (status, body) = send(
            account,
            "UpdateCluster",
            json!({"ClusterName": "cl-1", "Description": "updated"}),
        );
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["Cluster"]["Description"], "updated");

        let (status, body) = send(account, "DescribeClusters", json!({"ClusterName": "cl-1"}));
        assert_eq!(status, 200);
        assert_eq!(body["Clusters"][0]["Name"], "cl-1");

        let (status, body) = send(
            account,
            "DescribeClusters",
            json!({"ClusterName": "missing"}),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "ClusterNotFoundFault");

        let (status, body) = send(account, "DeleteCluster", json!({"ClusterName": "cl-1"}));
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["Cluster"]["Status"], "deleting");

        let (status, _) = send(
            account,
            "CreateCluster",
            json!({"ClusterName": "cl-1", "NodeType": "db.t4g.small", "ACLName": "open-access"}),
        );
        assert_eq!(status, 200);
    }

    #[test]
    fn duplicate_cluster_fault() {
        let account = next_account();
        create_cluster_ok(account, "dup-cl");
        let (status, body) = send(
            account,
            "CreateCluster",
            json!({"ClusterName": "dup-cl", "NodeType": "db.t4g.small"}),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "ClusterAlreadyExistsFault");
    }

    #[test]
    fn delete_missing_cluster_fault_carries_name() {
        let account = next_account();
        let (status, body) = send(
            account,
            "DeleteCluster",
            json!({"ClusterName": "ghost-cluster"}),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "ClusterNotFoundFault");
        assert_eq!(body["message"], "ghost-cluster");
    }

    #[test]
    fn snapshot_lifecycle_and_filters() {
        let account = next_account();
        create_cluster_ok(account, "snap-cl");
        let (status, body) = send(
            account,
            "CreateSnapshot",
            json!({"ClusterName": "snap-cl", "SnapshotName": "snap-1"}),
        );
        assert_eq!(status, 200, "{body}");
        let snap = &body["Snapshot"];
        assert_eq!(snap["Name"], "snap-1");
        assert_eq!(snap["Status"], "available");
        assert_eq!(snap["Source"], "manual");
        assert_eq!(snap["ClusterConfiguration"]["Name"], "snap-cl");

        // Duplicate snapshot name is rejected.
        let (status, body) = send(
            account,
            "CreateSnapshot",
            json!({"ClusterName": "snap-cl", "SnapshotName": "snap-1"}),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "SnapshotAlreadyExistsFault");

        // Snapshot of an unknown cluster.
        let (status, body) = send(
            account,
            "CreateSnapshot",
            json!({"ClusterName": "ghost", "SnapshotName": "snap-2"}),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "ClusterNotFoundFault");

        // Filtered describes.
        let (status, body) = send(
            account,
            "DescribeSnapshots",
            json!({"SnapshotName": "snap-1"}),
        );
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["Snapshots"][0]["Name"], "snap-1");

        let (status, body) = send(account, "DescribeSnapshots", json!({"ShowDetail": true}));
        assert_eq!(status, 200, "{body}");
        assert!(body["Snapshots"][0]["ClusterConfiguration"]
            .get("Shards")
            .is_some());
        // Plain describe exposes no Shards member.
        let (_, body) = send(account, "DescribeSnapshots", json!({}));
        assert!(body["Snapshots"][0]["ClusterConfiguration"]
            .get("Shards")
            .is_none());

        let (status, body) = send(account, "DeleteSnapshot", json!({"SnapshotName": "snap-1"}));
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["Snapshot"]["Status"], "deleting");

        let (status, body) = send(account, "DeleteSnapshot", json!({"SnapshotName": "snap-1"}));
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "SnapshotNotFoundFault");
    }

    #[test]
    fn delete_cluster_final_snapshot() {
        let account = next_account();
        create_cluster_ok(account, "final-cl");
        let (status, _) = send(
            account,
            "DeleteCluster",
            json!({"ClusterName": "final-cl", "FinalSnapshotName": "final-snap"}),
        );
        assert_eq!(status, 200);
        let (status, body) = send(
            account,
            "DescribeSnapshots",
            json!({"SnapshotName": "final-snap"}),
        );
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["Snapshots"][0]["Name"], "final-snap");
    }

    #[test]
    fn subnet_group_lifecycle_and_ec2_bridge() {
        let account = next_account();
        // The default group exists with live EC2 subnets.
        let (status, body) = send(account, "DescribeSubnetGroups", json!({}));
        assert_eq!(status, 200, "{body}");
        let default = body["SubnetGroups"]
            .as_array()
            .unwrap()
            .iter()
            .find(|g| g["Name"] == "default")
            .expect("default subnet group")
            .clone();
        assert!(!default["Subnets"].as_array().unwrap().is_empty());
        assert!(default["VpcId"].as_str().unwrap().starts_with("vpc-"));

        // Pick a real subnet from the bridge.
        let subnet_id = default["Subnets"][0]["Identifier"].as_str().unwrap();
        let (status, body) = send(
            account,
            "CreateSubnetGroup",
            json!({"SubnetGroupName": "sg-1", "Description": "d", "SubnetIds": [subnet_id]}),
        );
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["SubnetGroup"]["Name"], "sg-1");
        assert_eq!(body["SubnetGroup"]["Subnets"][0]["Identifier"], subnet_id);

        // Totally unknown subnets are rejected.
        let (status, body) = send(
            account,
            "CreateSubnetGroup",
            json!({"SubnetGroupName": "sg-2", "SubnetIds": ["subnet-00000000000000000"]}),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "InvalidSubnetError");

        let (status, body) = send(
            account,
            "DeleteSubnetGroup",
            json!({"SubnetGroupName": "sg-1"}),
        );
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["SubnetGroup"]["Name"], "sg-1");

        let (status, _) = send(
            account,
            "DeleteSubnetGroup",
            json!({"SubnetGroupName": "sg-1"}),
        );
        assert_eq!(status, 400);
    }

    #[test]
    fn delete_default_subnet_group_reserved() {
        let account = next_account();
        let (status, body) = send(
            account,
            "DeleteSubnetGroup",
            json!({"SubnetGroupName": "default"}),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "InvalidParameterValueException");

        // In-use subnet group.
        create_cluster_ok(account, "sginuse-cl");
        let (status, body) = send(
            account,
            "DeleteSubnetGroup",
            json!({"SubnetGroupName": "default"}),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "InvalidParameterValueException");
    }

    #[test]
    fn acl_and_user_operations() {
        let account = next_account();
        // Pre-seeded defaults are visible.
        let (status, body) = send(account, "DescribeACLs", json!({}));
        assert_eq!(status, 200, "{body}");
        assert!(body["ACLs"]
            .as_array()
            .unwrap()
            .iter()
            .any(|a| a["Name"] == "open-access"));

        let (_, body) = send(account, "DescribeUsers", json!({}));
        assert!(body["Users"]
            .as_array()
            .unwrap()
            .iter()
            .any(|u| u["Name"] == "default" && u["AccessString"] == "on ~* +@all"));

        let (status, body) = send(
            account,
            "CreateACL",
            json!({"ACLName": "acl-1", "UserNames": ["u1"]}),
        );
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["ACL"]["Name"], "acl-1");
        assert_eq!(body["ACL"]["Status"], "active");
        assert_eq!(body["ACL"]["UserNames"], json!(["u1"]));

        let (status, _) = send(account, "CreateACL", json!({"ACLName": "acl-1"}));
        assert_eq!(status, 400);

        let (status, body) = send(
            account,
            "UpdateACL",
            json!({"ACLName": "acl-1", "UserNamesToAdd": ["u2"], "UserNamesToRemove": ["u1"]}),
        );
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["ACL"]["UserNames"], json!(["u2"]));

        // Unknown ACL updates fault.
        let (status, body) = send(
            account,
            "UpdateACL",
            json!({"ACLName": "ghost", "UserNamesToAdd": ["u"]}),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "ACLNotFoundFault");

        let (status, body) = send(account, "DeleteACL", json!({"ACLName": "acl-1"}));
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["ACL"]["Status"], "deleting");

        let (status, body) = send(account, "DeleteACL", json!({"ACLName": "acl-1"}));
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "ACLNotFoundFault");
    }

    #[test]
    fn user_create_update_delete() {
        let account = next_account();
        let (status, body) = send(
            account,
            "CreateUser",
            json!({"UserName": "user-1", "AccessString": "on ~app* +@read",
                   "AuthenticationMode": {"Type": "password"}}),
        );
        assert_eq!(status, 200, "{body}");
        let user = &body["User"];
        assert_eq!(user["Name"], "user-1");
        assert_eq!(user["AccessString"], "on ~app* +@read");
        assert_eq!(user["Authentication"]["Type"], "password");

        let (status, body) = send(
            account,
            "UpdateUser",
            json!({"UserName": "user-1", "AccessString": "on ~* +@all"}),
        );
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["User"]["AccessString"], "on ~* +@all");

        let (status, body) = send(account, "DeleteUser", json!({"UserName": "user-1"}));
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["User"]["Status"], "deleting");

        let (status, body) = send(account, "DeleteUser", json!({"UserName": "user-1"}));
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "UserNotFoundFault");
    }

    #[test]
    fn parameter_group_operations() {
        let account = next_account();
        let (status, body) = send(
            account,
            "CreateParameterGroup",
            json!({"ParameterGroupName": "pg-1", "Family": "memorydb_redis7",
                   "Description": "test"}),
        );
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["ParameterGroup"]["Name"], "pg-1");
        assert_eq!(body["ParameterGroup"]["Family"], "memorydb_redis7");

        // The default family applies when Family is omitted.
        let (status, body) = send(
            account,
            "CreateParameterGroup",
            json!({"ParameterGroupName": "pg-2"}),
        );
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["ParameterGroup"]["Family"], "memorydb_redis7");

        let (status, _) = send(
            account,
            "CreateParameterGroup",
            json!({"ParameterGroupName": "pg-1"}),
        );
        assert_eq!(status, 400);

        // UpdateParameterGroup is a no-op on the stored group.
        let (status, body) = send(
            account,
            "UpdateParameterGroup",
            json!({"ParameterGroupName": "pg-1",
                   "ParameterNameValues": [{"ParameterName": "activedefrag", "ParameterValue": "yes"}]}),
        );
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["ParameterGroup"]["Name"], "pg-1");

        let (status, body) = send(
            account,
            "DescribeParameterGroups",
            json!({"ParameterGroupName": "pg-2"}),
        );
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["ParameterGroups"][0]["Name"], "pg-2");

        let (status, body) = send(
            account,
            "DeleteParameterGroup",
            json!({"ParameterGroupName": "pg-1"}),
        );
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["ParameterGroup"]["Name"], "pg-1");

        let (status, body) = send(
            account,
            "DeleteParameterGroup",
            json!({"ParameterGroupName": "pg-1"}),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "ParameterGroupNotFoundFault");
    }

    #[test]
    fn tag_lifecycle_and_membership() {
        let account = next_account();
        create_cluster_ok(account, "tag-cl");
        let (_, body) = send(
            account,
            "DescribeClusters",
            json!({"ClusterName": "tag-cl"}),
        );
        let arn = body["Clusters"][0]["ARN"].as_str().unwrap().to_string();

        let (status, body) = send(
            account,
            "TagResource",
            json!({"ResourceArn": arn, "Tags": [{"Key": "env", "Value": "test"}]}),
        );
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["TagList"][0]["Key"], "env");
        assert_eq!(body["TagList"][0]["Value"], "test");

        let (status, body) = send(account, "ListTags", json!({"ResourceArn": arn}));
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["TagList"].as_array().unwrap().len(), 1);

        // Untag with an unknown key is TagNotFoundFault.
        let (status, body) = send(
            account,
            "UntagResource",
            json!({"ResourceArn": arn, "TagKeys": ["missing"]}),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "TagNotFoundFault");

        let (status, _) = send(
            account,
            "UntagResource",
            json!({"ResourceArn": arn, "TagKeys": ["env"]}),
        );
        assert_eq!(status, 200);
        let (_, body) = send(account, "ListTags", json!({"ResourceArn": arn}));
        assert_eq!(body["TagList"].as_array().unwrap().len(), 0);

        // Unknown ARNs pick the fault by resource-type substring.
        let (status, body) = send(
            account,
            "TagResource",
            json!({"ResourceArn": "arn:aws:memorydb:us-east-1:1:cluster/ghost",
                   "Tags": [{"Key": "a", "Value": "b"}]}),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "ClusterNotFoundFault");
        assert_eq!(body["message"], "ghost is not present");

        let (status, body) = send(
            account,
            "TagResource",
            json!({"ResourceArn": "arn:aws:memorydb:us-east-1:1:snapshot/ghost",
                   "Tags": [{"Key": "a", "Value": "b"}]}),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "SnapshotNotFoundFault");
    }

    #[test]
    fn static_describe_lists() {
        let account = next_account();
        for (op, key) in [
            ("DescribeServiceUpdates", "ServiceUpdates"),
            ("DescribeEvents", "Events"),
            ("DescribeReservedNodes", "ReservedNodes"),
            ("DescribeReservedNodesOfferings", "ReservedNodesOfferings"),
        ] {
            let (status, body) = send(account, op, json!({}));
            assert_eq!(status, 200, "{op}: {body}");
            assert!(body[key].as_array().unwrap().is_empty(), "{op}");
        }
        let (status, body) = send(account, "DescribeEngineVersions", json!({}));
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["EngineVersions"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn engine_version_mapping() {
        let account = next_account();
        let (status, body) = send(
            account,
            "CreateCluster",
            json!({"ClusterName": "eng-cl", "NodeType": "db.t4g.small",
                   "EngineVersion": "6.2", "ACLName": "open-access"}),
        );
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["Cluster"]["EngineVersion"], "6.2");
        assert_eq!(body["Cluster"]["EnginePatchVersion"], "6.2.6");

        // Unknown versions are rewritten to 7.1.
        let (status, body) = send(
            account,
            "CreateCluster",
            json!({"ClusterName": "eng-cl-2", "NodeType": "db.t4g.small",
                   "EngineVersion": "9.9", "ACLName": "open-access"}),
        );
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["Cluster"]["EngineVersion"], "7.1");
        assert_eq!(body["Cluster"]["EnginePatchVersion"], "7.1.1");
    }

    #[test]
    fn tls_off_clusters_use_open_access_acl() {
        let account = next_account();
        let (status, body) = send(
            account,
            "CreateCluster",
            json!({"ClusterName": "tls-off", "NodeType": "db.t4g.small",
                   "ACLName": "custom-acl", "TLSEnabled": false}),
        );
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["Cluster"]["ACLName"], "open-access");
        assert_eq!(body["Cluster"]["TLSEnabled"], json!(false));
    }

    #[test]
    fn operations_absent_from_moto_fork_answer_501() {
        let account = next_account();
        for op in [
            "BatchUpdateCluster",
            "CopySnapshot",
            "CreateMultiRegionCluster",
            "DeleteMultiRegionCluster",
            "DescribeMultiRegionClusters",
            "DescribeMultiRegionParameterGroups",
            "DescribeMultiRegionParameters",
            "DescribeParameters",
            "FailoverShard",
            "ListAllowedMultiRegionClusterUpdates",
            "ListAllowedNodeTypeUpdates",
            "PurchaseReservedNodesOffering",
            "ResetParameterGroup",
            "UpdateMultiRegionCluster",
            "UpdateSubnetGroup",
        ] {
            let (status, body) = send(account, op, json!({}));
            assert_eq!(status, 501, "{op} must answer 501: {body}");
            assert_eq!(body["__type"], "NotImplemented", "{op}");
        }
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
}
