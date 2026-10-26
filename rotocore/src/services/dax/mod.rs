//! AWS DynamoDB Accelerator (DAX) backend — moto-equivalent (json protocol).
//!
//! Replaces the moto sidecar for `dax` (`X-Amz-Target: AmazonDAXV3.<Op>` on
//! `application/x-amz-json-1.1`). State is per-(account, region), mirroring
//! moto's `DAXBackend` (moto/dax): clusters with node/endpoint shapes, the
//! seeded `default.dax1.0` parameter group, subnet groups, the cluster-level
//! tagging service, and the describe-events list. No probe file exists for
//! this service — the deployed moto fork checkout (`moto/dax/`, commit
//! `2016563fd`) is the ground truth and all 21 botocore operations have
//! backed handlers there; tests/compatibility/test_dax_compat.py is the spec.
//!
//! Fidelity notes: `CreateCluster` validates the IAM ARN's colon count and
//! the cluster-name regex before the backend answers; clusters carry moto's
//! `ManagedState` ("dax::cluster", manual progression, `times: 4`) so a new
//! cluster reports `creating` until its fourth describe advances it to
//! `available` (which is when `Nodes` join the wire shape); `describe_clusters`
//! paginates with moto's botocore-style `{"uniqueAttributes": arn}` base64
//! token; moto's duplicate Create overwrites without an error, and
//! replication-factor changes are applied without bounds — both retained.

use crate::proto::{json_error, now_unix, parse_json_body};
use crate::store::{self, ScopeKey};
use crate::{Request, Response};
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use parking_lot::RwLock;
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use uuid::Uuid;

const PAGE_LIMIT_DEFAULT: usize = 100;
const DAX_PORT: i64 = 8111;
/// moto's seeded default parameter group.
const DEFAULT_PARAMETER_GROUP: &str = "default.dax1.0";

const INVALID_CLUSTER_NAME_MSG: &str = "Cluster ID specified is not a valid identifier. Identifiers must begin with a letter; must contain only ASCII letters, digits, and hyphens; and must not end with a hyphen or contain two consecutive hyphens.";

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

type Stores = Arc<RwLock<HashMap<ScopeKey, DaxStore>>>;

fn stores() -> Stores {
    store::slot::<DaxStore>("dax")
}

/// Per-(account, region) in-memory state for Amazon DAX.
#[derive(Default)]
struct DaxStore {
    /// cluster name -> cluster ("deleted" members are purged on read).
    clusters: BTreeMap<String, Cluster>,
    parameter_groups: BTreeMap<String, ParameterGroup>,
    subnet_groups: BTreeMap<String, SubnetGroup>,
    /// Events in moto's flattened dict shape.
    events: Vec<Value>,
    /// arn -> {tag key: value} — moto's `TaggingService`.
    tags: BTreeMap<String, BTreeMap<String, Option<String>>>,
}

impl DaxStore {
    /// moto's backend constructor seeds one default parameter group.
    fn seed_default_parameter_group(&mut self) {
        self.parameter_groups
            .entry(DEFAULT_PARAMETER_GROUP.to_string())
            .or_insert_with(|| {
                ParameterGroup::new(
                    DEFAULT_PARAMETER_GROUP,
                    json!("Default parameter group for dax1.0"),
                )
            });
    }

    /// moto's `clusters` property getter: every read applies each cluster's
    /// manual transition (4 ticks -> next state) and drops "deleted" ones.
    fn refresh_clusters(&mut self) {
        for cluster in self.clusters.values_mut() {
            cluster.advance_status();
        }
        self.clusters.retain(|_, c| c.status != "deleted");
    }
}

fn with_store(req: &Request, f: impl FnOnce(&mut DaxStore) -> Response) -> Response {
    let slot = stores();
    let mut map = slot.write();
    let st = map.entry((req.account, req.region.clone())).or_default();
    st.seed_default_parameter_group();
    f(st)
}

// ---------------------------------------------------------------------------
// Records
// ---------------------------------------------------------------------------

/// moto's `DaxCluster` with its `ManagedState` transitions
/// [("creating", "available"), ("deleting", "deleted")], manual progression.
struct Cluster {
    name: String,
    arn: String,
    description: Value,
    node_type: String,
    replication_factor: i64,
    cluster_hex: String,
    region: String,
    nodes: Vec<Node>,
    preferred_maintenance_window: String,
    subnet_group: String,
    iam_role_arn: String,
    parameter_group_name: String,
    security_groups: Vec<Value>,
    sse_enabled: bool,
    encryption_type: String,
    status: String,
    tick: u32,
}

impl Cluster {
    /// Reads its own initialization inputs off the request body, mirroring
    /// moto's `create_cluster` parameter plumb-through.
    fn new(account: u64, region: &str, name: &str, body: &Value) -> Self {
        let sse_enabled = body
            .get("SSESpecification")
            .and_then(|s| s.get("Enabled"))
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let encryption_type = body
            .get("ClusterEndpointEncryptionType")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .unwrap_or("NONE")
            .to_string();
        let replication_factor = body
            .get("ReplicationFactor")
            .and_then(Value::as_i64)
            .unwrap_or(1);
        let cluster_hex = random_hex(6);
        Cluster {
            name: name.to_string(),
            arn: format!(
                "arn:{}:dax:{region}:{account}:cache/{name}",
                partition(region)
            ),
            description: body.get("Description").cloned().unwrap_or(Value::Null),
            node_type: as_str(body, "NodeType").to_string(),
            replication_factor,
            cluster_hex: cluster_hex.clone(),
            region: region.to_string(),
            nodes: (0..replication_factor)
                .map(|idx| Node::new(name, &cluster_hex, region, idx))
                .collect(),
            preferred_maintenance_window: "thu:23:30-fri:00:30".to_string(),
            subnet_group: "default".to_string(),
            iam_role_arn: as_str(body, "IamRoleArn").to_string(),
            // DaxCluster seeds a default `DaxParameterGroup()`.
            parameter_group_name: DEFAULT_PARAMETER_GROUP.to_string(),
            security_groups: vec![json!({
                "SecurityGroupIdentifier": format!("sg-{}", random_hex(10)),
                "Status": "active",
            })],
            sse_enabled,
            encryption_type,
            // ManagedState: the initial status is the first transition's source.
            status: "creating".to_string(),
            tick: 0,
        }
    }

    fn advance(&mut self) {
        self.tick += 1;
    }

    /// moto's `status` property under the `dax::cluster` transition
    /// (progression "manual", times 4): advance one step per four ticks.
    fn advance_status(&mut self) -> String {
        if self.tick >= 4 {
            self.tick = 0;
            match self.status.as_str() {
                "creating" => self.status = "available".to_string(),
                "deleting" => self.status = "deleted".to_string(),
                _ => {}
            }
        }
        self.status.clone()
    }

    /// moto's `DaxCluster.delete`: move to "deleting" ("deleted" arrives via
    /// ticks and is purged from the map by the `clusters` property reader).
    fn delete(&mut self) {
        self.status = "deleting".to_string();
    }

    /// moto's `DaxCluster.to_json`: the full node-bearing shape only once
    /// the cluster reaches `available` (`use_full_repr`). Not named `to_*`
    /// because the status advance needs `&mut self`.
    fn render(&mut self) -> Value {
        let status = self.advance_status();
        let use_full_repr = status == "available";
        let mut endpoint = json!({ "Port": DAX_PORT });
        if use_full_repr {
            let address = format!(
                "{}.{}.dax-clusters.{}.amazonaws.com",
                self.name, self.cluster_hex, self.region
            );
            endpoint["Address"] = json!(address);
            endpoint["URL"] = json!(format!("dax://{address}"));
        }
        let mut out = json!({
            "ClusterName": self.name,
            "Description": self.description,
            "ClusterArn": self.arn,
            "TotalNodes": self.replication_factor,
            "ActiveNodes": 0,
            "NodeType": self.node_type,
            "Status": status,
            "ClusterDiscoveryEndpoint": endpoint,
            "PreferredMaintenanceWindow": self.preferred_maintenance_window,
            "SubnetGroup": self.subnet_group,
            "IamRoleArn": self.iam_role_arn,
            "ParameterGroup": {
                "ParameterGroupName": self.parameter_group_name,
                "ParameterApplyStatus": "in-sync",
                "NodeIdsToReboot": [],
            },
            "SSEDescription": { "Status": if self.sse_enabled { "ENABLED" } else { "DISABLED" } },
            "ClusterEndpointEncryptionType": self.encryption_type,
            "SecurityGroups": self.security_groups,
        });
        if use_full_repr {
            let nodes: Vec<Value> = self.nodes.iter().map(Node::to_json).collect();
            out["Nodes"] = json!(nodes);
        }
        out
    }
}

struct Node {
    node_id: String,
    address: String,
    create_time: f64,
    availability_zone: String,
}

impl Node {
    /// moto's `DaxNode`: ids `name-a`, `name-b`, ... (moto runs past `z` for
    /// absurd replication factors — `chr(ord('a') + index)`).
    fn new(name: &str, cluster_hex: &str, region: &str, index: i64) -> Self {
        let node_id = format!(
            "{name}-{}",
            char::from_u32(b'a' as u32 + index as u32).unwrap_or('?')
        );
        Node {
            address: format!("{node_id}.{cluster_hex}.nodes.dax-clusters.{region}.amazonaws.com"),
            node_id,
            create_time: now_unix(),
            availability_zone: format!("{region}a"),
        }
    }

    fn to_json(&self) -> Value {
        json!({
            "NodeId": self.node_id,
            "Endpoint": { "Address": self.address, "Port": DAX_PORT },
            "NodeCreateTime": self.create_time,
            "AvailabilityZone": self.availability_zone,
            "NodeStatus": "available",
            "ParameterGroupStatus": "in-sync",
        })
    }
}

struct ParameterGroup {
    name: String,
    description: Value,
    /// Each group owns its own copy of the default parameters.
    parameters: Vec<Value>,
}

impl ParameterGroup {
    fn new(name: &str, description: Value) -> Self {
        ParameterGroup {
            name: name.to_string(),
            description,
            parameters: default_parameters(),
        }
    }

    /// moto's `DaxParameterGroup.to_describe_json` (description may be null
    /// when CreateParameterGroup carried an explicit null — `params.get`
    /// only defaults absent values to `""`).
    fn to_describe_json(&self) -> Value {
        json!({
            "ParameterGroupName": self.name,
            "Description": self.description,
        })
    }
}

struct SubnetGroup {
    name: String,
    description: Value,
    subnet_ids: Vec<String>,
    vpc_id: String,
    region: String,
}

impl SubnetGroup {
    fn new(name: &str, description: Value, subnet_ids: Vec<String>, region: &str) -> Self {
        SubnetGroup {
            name: name.to_string(),
            description,
            subnet_ids,
            vpc_id: format!("vpc-{}", random_hex(8)),
            region: region.to_string(),
        }
    }

    fn to_json(&self) -> Value {
        json!({
            "SubnetGroupName": self.name,
            "Description": self.description,
            "VpcId": self.vpc_id,
            "Subnets": self.subnet_ids.iter().map(|sid| json!({
                "SubnetIdentifier": sid,
                "SubnetAvailabilityZone": format!("{}a", self.region),
            })).collect::<Vec<_>>(),
        })
    }
}

/// moto's `DEFAULT_PARAMETERS` (subset of what AWS returns).
fn default_parameters() -> Vec<Value> {
    json!([
        {
            "ParameterName": "query-ttl-millis",
            "ParameterType": "DEFAULT",
            "ParameterValue": "300000",
            "NodeTypeSpecificValues": [],
            "Description": "Duration in milliseconds for queries to remain cached",
            "Source": "user",
            "DataType": "integer",
            "AllowedValues": "0-",
            "IsModifiable": "TRUE",
            "ChangeType": "IMMEDIATE",
        },
        {
            "ParameterName": "record-ttl-millis",
            "ParameterType": "DEFAULT",
            "ParameterValue": "300000",
            "NodeTypeSpecificValues": [],
            "Description": "Duration in milliseconds for records to remain valid in cache",
            "Source": "user",
            "DataType": "integer",
            "AllowedValues": "0-",
            "IsModifiable": "TRUE",
            "ChangeType": "IMMEDIATE",
        },
    ])
    .as_array()
    .expect("literal array")
    .clone()
}

// ---------------------------------------------------------------------------
// dispatch
// ---------------------------------------------------------------------------

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "dax" {
        return None;
    }
    Some(with_store(req, |st| route(req, st)))
}

fn route(req: &Request, st: &mut DaxStore) -> Response {
    let body = parse_json_body(&req.body);
    match req.operation.as_str() {
        // --- clusters -------------------------------------------------------
        "CreateCluster" => create_cluster(st, &body, req.account, &req.region),
        "DeleteCluster" => delete_cluster(st, &body),
        "DescribeClusters" => describe_clusters(st, &body),
        "UpdateCluster" => update_cluster(st, &body),
        "RebootNode" => reboot_node(st, &body),
        "IncreaseReplicationFactor" => increase_replication_factor(st, &body),
        "DecreaseReplicationFactor" => decrease_replication_factor(st, &body),
        // --- tags -------------------------------------------------------------
        "ListTags" => list_tags(st, &body),
        "TagResource" => tag_resource(st, &body),
        "UntagResource" => untag_resource(st, &body),
        // --- parameter groups -------------------------------------------------
        "CreateParameterGroup" => create_parameter_group(st, &body),
        "DeleteParameterGroup" => delete_parameter_group(st, &body),
        "DescribeParameterGroups" => describe_parameter_groups(st, &body),
        "DescribeParameters" => describe_parameters(st, &body),
        "DescribeDefaultParameters" => ok(json!({ "Parameters": default_parameters() })),
        "UpdateParameterGroup" => update_parameter_group(st, &body),
        // --- subnet groups ----------------------------------------------------
        "CreateSubnetGroup" => create_subnet_group(st, &body, &req.region),
        "DeleteSubnetGroup" => delete_subnet_group(st, &body),
        "DescribeSubnetGroups" => describe_subnet_groups(st, &body),
        "UpdateSubnetGroup" => update_subnet_group(st, &body),
        // --- events -----------------------------------------------------------
        "DescribeEvents" => describe_events(st, &body),
        other => json_error(
            501,
            "NotImplemented",
            &format!("The {other} action has not been implemented"),
        ),
    }
}

// ---------------------------------------------------------------------------
// Operation helpers — clusters
// ---------------------------------------------------------------------------

fn create_cluster(st: &mut DaxStore, body: &Value, account: u64, region: &str) -> Response {
    let name = as_str(body, "ClusterName");
    let iam_role_arn = as_str(body, "IamRoleArn");
    // moto validates the ARN before the name.
    if let Err(resp) = validate_arn(iam_role_arn) {
        return resp;
    }
    if !valid_cluster_name(name) {
        return invalid_parameter(INVALID_CLUSTER_NAME_MSG);
    }
    let cluster = Cluster::new(account, region, name, body);
    let arn = cluster.arn.clone();
    // moto: a duplicate Create silently overwrites the stored cluster.
    st.clusters.insert(name.to_string(), cluster);
    // moto tags after inserting; an empty Tags list still seeds the tagger
    // row (`if tags is None: return` only skips nulls).
    tags_add(st, &arn, body.get("Tags").and_then(Value::as_array));
    st.events.push(cluster_event(
        name,
        "CLUSTER",
        &format!("Cluster {name} created."),
    ));
    ok(json!({
        "Cluster": st
            .clusters
            .get_mut(name)
            .expect("just inserted")
            .render()
    }))
}

fn delete_cluster(st: &mut DaxStore, body: &Value) -> Response {
    let name = as_str(body, "ClusterName");
    st.refresh_clusters();
    let Some(cluster) = st.clusters.get_mut(name) else {
        return cluster_not_found(None);
    };
    cluster.delete();
    st.events.push(cluster_event(
        name,
        "CLUSTER",
        &format!("Cluster {name} deleted."),
    ));
    // moto's trailing `self.clusters[cluster_name]` re-reads the property,
    // which would purge a cluster whose ticks finished "deleted" (KeyError ->
    // bare 500). Ticks are always < 4 here in the natural flow, so the
    // cluster is still present.
    st.refresh_clusters();
    let Some(cluster) = st.clusters.get_mut(name) else {
        return crate::proto::moto_crash_response();
    };
    ok(json!({ "Cluster": cluster.render() }))
}

fn describe_clusters(st: &mut DaxStore, body: &Value) -> Response {
    let cluster_names = as_str_list(body, "ClusterNames");
    // moto validates every requested name before touching the store.
    for name in &cluster_names {
        if !valid_cluster_name(name) {
            return invalid_parameter(INVALID_CLUSTER_NAME_MSG);
        }
    }
    st.refresh_clusters();
    let names = if cluster_names.is_empty() {
        st.clusters.keys().cloned().collect::<Vec<_>>()
    } else {
        cluster_names
    };
    for name in &names {
        if st.clusters.contains_key(name) {
            st.clusters.get_mut(name).expect("checked").advance();
        }
    }
    st.refresh_clusters();
    for name in &names {
        if !st.clusters.contains_key(name) {
            return cluster_not_found(Some(name));
        }
    }
    // moto's `@paginate` runs after the backend call: token by cluster ARN.
    let entries: Vec<(String, String)> = st
        .clusters
        .values()
        .filter(|c| names.contains(&c.name))
        .map(|c| (c.arn.clone(), c.name.clone()))
        .collect();
    let (page_names, token) =
        match paginate_entries(&entries, body.get("MaxResults"), body.get("NextToken")) {
            Ok(result) => result,
            Err(resp) => return resp,
        };
    let display: Vec<Value> = page_names
        .iter()
        .map(|name| {
            st.clusters
                .get_mut(name)
                .expect("listed cluster stays present")
                .render()
        })
        .collect();
    ok(json!({
        "Clusters": display,
        // moto emits the key even when done (null on the wire).
        "NextToken": token.map(Value::from).unwrap_or(Value::Null),
    }))
}

fn update_cluster(st: &mut DaxStore, body: &Value) -> Response {
    let name = as_str(body, "ClusterName");
    st.refresh_clusters();
    let Some(cluster) = st.clusters.get_mut(name) else {
        return cluster_not_found(None);
    };
    // moto: each `is not None` parameter overwrites its field.
    if let Some(description) = body.get("Description").filter(|d| !d.is_null()) {
        cluster.description = description.clone();
    }
    if let Some(w) = body
        .get("PreferredMaintenanceWindow")
        .and_then(Value::as_str)
    {
        cluster.preferred_maintenance_window = w.to_string();
    }
    if !body
        .get("ParameterGroupName")
        .map(Value::is_null)
        .unwrap_or(true)
    {
        let pg_name = as_str(body, "ParameterGroupName").to_string();
        if !st.parameter_groups.contains_key(&pg_name) {
            return parameter_group_not_found(Some(&pg_name));
        }
        cluster.parameter_group_name = pg_name;
    }
    if let Some(sg_ids) = body.get("SecurityGroupIds").filter(|v| !v.is_null()) {
        cluster.security_groups = sg_ids
            .as_array()
            .map(|list| {
                list.iter()
                    .map(|id| {
                        json!({
                            "SecurityGroupIdentifier": id.as_str().unwrap_or_default(),
                            "Status": "active",
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
    }
    st.events.push(cluster_event(
        name,
        "CLUSTER",
        &format!("Cluster {name} updated."),
    ));
    ok(json!({
        "Cluster": st.clusters.get_mut(name).expect("checked").render()
    }))
}

fn reboot_node(st: &mut DaxStore, body: &Value) -> Response {
    let name = as_str(body, "ClusterName");
    st.refresh_clusters();
    let node_id = as_str(body, "NodeId");
    let found = st
        .clusters
        .get(name)
        .map(|c| c.nodes.iter().any(|n| n.node_id == node_id));
    match found {
        None => return cluster_not_found(None),
        Some(false) => {
            return json_error(
                400,
                "NodeNotFoundFault",
                &format!("Node {node_id} not found."),
            )
        }
        Some(true) => {}
    }
    st.events.push(cluster_event(
        node_id,
        "NODE",
        &format!("Node {node_id} rebooted."),
    ));
    ok(json!({
        "Cluster": st.clusters.get_mut(name).expect("checked").render()
    }))
}

fn increase_replication_factor(st: &mut DaxStore, body: &Value) -> Response {
    let name = as_str(body, "ClusterName");
    st.refresh_clusters();
    let Some(cluster) = st.clusters.get_mut(name) else {
        return cluster_not_found(None);
    };
    let new_rf = body
        .get("NewReplicationFactor")
        .and_then(Value::as_i64)
        .unwrap_or(cluster.replication_factor);
    // moto: appends one node per new slot (no bounds check).
    let start = cluster.replication_factor;
    for idx in start..new_rf {
        cluster
            .nodes
            .push(Node::new(name, &cluster.cluster_hex, &cluster.region, idx));
    }
    cluster.replication_factor = new_rf;
    ok(json!({
        "Cluster": st.clusters.get_mut(name).expect("checked").render()
    }))
}

fn decrease_replication_factor(st: &mut DaxStore, body: &Value) -> Response {
    let name = as_str(body, "ClusterName");
    st.refresh_clusters();
    let Some(cluster) = st.clusters.get_mut(name) else {
        return cluster_not_found(None);
    };
    let node_ids_to_remove = as_str_list(body, "NodeIdsToRemove");
    // moto: named removals win; otherwise the list is truncated.
    if !node_ids_to_remove.is_empty() {
        cluster
            .nodes
            .retain(|n| !node_ids_to_remove.contains(&n.node_id));
    } else {
        let new_rf = body
            .get("NewReplicationFactor")
            .and_then(Value::as_i64)
            .unwrap_or(cluster.replication_factor);
        cluster.nodes.truncate(new_rf.max(0) as usize);
    }
    cluster.replication_factor = body
        .get("NewReplicationFactor")
        .and_then(Value::as_i64)
        .unwrap_or(cluster.replication_factor);
    ok(json!({
        "Cluster": st.clusters.get_mut(name).expect("checked").render()
    }))
}

// ---------------------------------------------------------------------------
// Operation helpers — tags
// ---------------------------------------------------------------------------

/// moto's `list_tags`/`tag_resource`/`untag_resource`: the resource can be the
/// name or the full ARN; both resolve to the segment after the final '/'.
/// Absent clusters raise `ClusterNotFoundFault` (no name in the message).
fn cluster_arn_from_resource_name(st: &DaxStore, resource_name: &str) -> Option<String> {
    let name = resource_name.rsplit('/').next().unwrap_or("");
    st.clusters.get(name).map(|c| c.arn.clone())
}

fn list_tags(st: &mut DaxStore, body: &Value) -> Response {
    let Some(arn) = cluster_arn_from_resource_name(st, as_str(body, "ResourceName")) else {
        return cluster_not_found(None);
    };
    ok(tags_for(&st.tags, &arn))
}

fn tag_resource(st: &mut DaxStore, body: &Value) -> Response {
    let Some(arn) = cluster_arn_from_resource_name(st, as_str(body, "ResourceName")) else {
        return cluster_not_found(None);
    };
    tags_add(st, &arn, body.get("Tags").and_then(Value::as_array));
    ok(json!({ "Tags": tags_for(&st.tags, &arn)["Tags"].clone() }))
}

fn untag_resource(st: &mut DaxStore, body: &Value) -> Response {
    let Some(arn) = cluster_arn_from_resource_name(st, as_str(body, "ResourceName")) else {
        return cluster_not_found(None);
    };
    if let Some(keys) = body.get("TagKeys").and_then(Value::as_array) {
        if let Some(row) = st.tags.get_mut(&arn) {
            for key in keys.iter().filter_map(Value::as_str) {
                row.remove(key);
            }
        }
    }
    ok(json!({ "Tags": tags_for(&st.tags, &arn)["Tags"].clone() }))
}

fn tags_for(tags: &BTreeMap<String, BTreeMap<String, Option<String>>>, arn: &str) -> Value {
    let list: Vec<Value> = tags
        .get(arn)
        .map(|row| {
            row.iter()
                .map(|(k, v)| json!({ "Key": k, "Value": tag_value(v.as_deref()) }))
                .collect()
        })
        .unwrap_or_default();
    json!({ "Tags": list })
}

/// moto's `TaggingService.tag_resource`: absent `Value` members store null.
fn tags_add(st: &mut DaxStore, arn: &str, tags: Option<&Vec<Value>>) {
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

fn tag_value(value: Option<&str>) -> Value {
    match value {
        Some(v) => json!(v),
        None => Value::Null,
    }
}

// ---------------------------------------------------------------------------
// Operation helpers — parameter groups
// ---------------------------------------------------------------------------

fn create_parameter_group(st: &mut DaxStore, body: &Value) -> Response {
    let name = as_str(body, "ParameterGroupName");
    if st.parameter_groups.contains_key(name) {
        return json_error(
            400,
            "ParameterGroupAlreadyExistsFault",
            &format!("ParameterGroup {name} already exists."),
        );
    }
    let description = body.get("Description").cloned().unwrap_or(json!(""));
    st.parameter_groups
        .insert(name.to_string(), ParameterGroup::new(name, description));
    st.events.push(cluster_event(
        name,
        "PARAMETER_GROUP",
        &format!("ParameterGroup {name} created."),
    ));
    ok(json!({ "ParameterGroup": st.parameter_groups[name].to_describe_json() }))
}

fn delete_parameter_group(st: &mut DaxStore, body: &Value) -> Response {
    let name = as_str(body, "ParameterGroupName");
    if st.parameter_groups.remove(name).is_none() {
        return parameter_group_not_found(Some(name));
    }
    ok(json!({
        "DeletionMessage": format!("ParameterGroup {name} has been deleted."),
    }))
}

fn describe_parameter_groups(st: &mut DaxStore, body: &Value) -> Response {
    let group_names = as_str_list(body, "ParameterGroupNames");
    if !group_names.is_empty() {
        if let Some(missing) = group_names
            .iter()
            .find(|n| !st.parameter_groups.contains_key(*n))
        {
            return parameter_group_not_found(Some(missing));
        }
    }
    let groups: Vec<Value> = if group_names.is_empty() {
        st.parameter_groups
            .values()
            .map(|g| g.to_describe_json())
            .collect()
    } else {
        group_names
            .iter()
            .filter_map(|name| st.parameter_groups.get(name))
            .map(|g| g.to_describe_json())
            .collect()
    };
    ok(json!({ "ParameterGroups": groups }))
}

fn describe_parameters(st: &mut DaxStore, body: &Value) -> Response {
    let name = as_str(body, "ParameterGroupName");
    let Some(pg) = st.parameter_groups.get(name) else {
        return parameter_group_not_found(Some(name));
    };
    // moto: only a truthy Source filters.
    let source = body
        .get("Source")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty());
    let parameters: Vec<Value> = pg
        .parameters
        .iter()
        .filter(|p| {
            source
                .map(|s| p.get("Source").and_then(Value::as_str) == Some(s))
                .unwrap_or(true)
        })
        .cloned()
        .collect();
    ok(json!({ "Parameters": parameters }))
}

fn update_parameter_group(st: &mut DaxStore, body: &Value) -> Response {
    let name = as_str(body, "ParameterGroupName");
    let Some(pg) = st.parameter_groups.get_mut(name) else {
        return parameter_group_not_found(Some(name));
    };
    if let Some(values) = body.get("ParameterNameValues").and_then(Value::as_array) {
        for pnv in values {
            let pname = pnv
                .get("ParameterName")
                .and_then(Value::as_str)
                .unwrap_or("");
            let pvalue = pnv
                .get("ParameterValue")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            for param in pg.parameters.iter_mut() {
                if param.get("ParameterName").and_then(Value::as_str) == Some(pname) {
                    if let Some(obj) = param.as_object_mut() {
                        obj.insert("ParameterValue".into(), json!(pvalue));
                    }
                    break;
                }
            }
        }
    }
    ok(json!({ "ParameterGroup": pg.to_describe_json() }))
}

// ---------------------------------------------------------------------------
// Operation helpers — subnet groups
// ---------------------------------------------------------------------------

fn create_subnet_group(st: &mut DaxStore, body: &Value, region: &str) -> Response {
    let name = as_str(body, "SubnetGroupName");
    if st.subnet_groups.contains_key(name) {
        return json_error(
            400,
            "SubnetGroupAlreadyExistsFault",
            &format!("SubnetGroup {name} already exists."),
        );
    }
    let description = body.get("Description").cloned().unwrap_or(json!(""));
    let sg = SubnetGroup::new(name, description, as_str_list(body, "SubnetIds"), region);
    st.subnet_groups.insert(name.to_string(), sg);
    st.events.push(cluster_event(
        name,
        "SUBNET_GROUP",
        &format!("SubnetGroup {name} created."),
    ));
    ok(json!({ "SubnetGroup": st.subnet_groups[name].to_json() }))
}

fn delete_subnet_group(st: &mut DaxStore, body: &Value) -> Response {
    let name = as_str(body, "SubnetGroupName");
    if st.subnet_groups.remove(name).is_none() {
        return subnet_group_not_found(name);
    }
    ok(json!({
        "DeletionMessage": format!("SubnetGroup {name} has been deleted."),
    }))
}

fn describe_subnet_groups(st: &mut DaxStore, body: &Value) -> Response {
    let group_names = as_str_list(body, "SubnetGroupNames");
    if !group_names.is_empty() {
        if let Some(missing) = group_names
            .iter()
            .find(|n| !st.subnet_groups.contains_key(*n))
        {
            return subnet_group_not_found(missing);
        }
    }
    let groups: Vec<Value> = if group_names.is_empty() {
        st.subnet_groups
            .values()
            .map(SubnetGroup::to_json)
            .collect()
    } else {
        group_names
            .iter()
            .filter_map(|name| st.subnet_groups.get(name))
            .map(SubnetGroup::to_json)
            .collect()
    };
    ok(json!({ "SubnetGroups": groups }))
}

fn update_subnet_group(st: &mut DaxStore, body: &Value) -> Response {
    let name = as_str(body, "SubnetGroupName");
    let Some(sg) = st.subnet_groups.get_mut(name) else {
        return subnet_group_not_found(name);
    };
    if let Some(desc) = body.get("Description").filter(|d| !d.is_null()) {
        sg.description = desc.clone();
    }
    if let Some(ids) = body.get("SubnetIds").filter(|d| !d.is_null()) {
        sg.subnet_ids = ids
            .as_array()
            .map(|list| {
                list.iter()
                    .filter_map(Value::as_str)
                    .map(String::from)
                    .collect()
            })
            .unwrap_or_default();
    }
    ok(json!({ "SubnetGroup": sg.to_json() }))
}

// ---------------------------------------------------------------------------
// Operation helpers — events
// ---------------------------------------------------------------------------

fn describe_events(st: &mut DaxStore, body: &Value) -> Response {
    // moto: only truthy filters apply.
    let source_name = body
        .get("SourceName")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty());
    let source_type = body
        .get("SourceType")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty());
    let events: Vec<Value> = st
        .events
        .iter()
        .filter(|e| {
            source_name
                .map(|s| e["SourceName"].as_str() == Some(s))
                .unwrap_or(true)
        })
        .filter(|e| {
            source_type
                .map(|s| e["SourceType"].as_str() == Some(s))
                .unwrap_or(true)
        })
        .cloned()
        .collect();
    ok(json!({ "Events": events }))
}

fn cluster_event(source_name: &str, source_type: &str, message: &str) -> Value {
    json!({
        "SourceName": source_name,
        "SourceType": source_type,
        "Message": message,
        "Date": now_unix(),
    })
}

// ---------------------------------------------------------------------------
// Pagination — moto's `@paginate` decorator with botocore-style markers
// (`{"uniqueAttributes": arn}` base64-JSON). moto additionally embeds a
// process-salted parameter checksum, which cannot be reproduced here; token
// decoding tolerates its absence (same convention as wafv2).
// ---------------------------------------------------------------------------

fn paginate_entries(
    entries: &[(String, String)],
    limit: Option<&Value>,
    marker: Option<&Value>,
) -> Result<(Vec<String>, Option<String>), Response> {
    // entries: (cluster arn, cluster name), in the store's stable order.
    let arns: Vec<&String> = entries.iter().map(|(arn, _)| arn).collect();
    let max_results = limit
        .and_then(Value::as_u64)
        .filter(|l| *l > 0)
        .map(|l| l as usize)
        .unwrap_or(PAGE_LIMIT_DEFAULT);
    let index_start = match marker.and_then(Value::as_str).filter(|m| !m.is_empty()) {
        None => 0,
        Some(token) => {
            let decoded = B64
                .decode(token)
                .ok()
                .and_then(|b| serde_json::from_slice::<Value>(&b).ok())
                .and_then(|v| {
                    v.get("uniqueAttributes")
                        .and_then(Value::as_str)
                        .map(String::from)
                });
            let attribute = decoded.ok_or_else(|| invalid_token("Invalid token"))?;
            match arns.iter().position(|arn| **arn == attribute) {
                Some(pos) => pos,
                // moto: `InvalidToken("Resource not found!")`.
                None => return Err(invalid_token("Resource not found!")),
            }
        }
    };
    let index_end = (index_start + max_results).min(entries.len());
    let page: Vec<String> = entries[index_start..index_end]
        .iter()
        .map(|(_, name)| name.clone())
        .collect();
    let next = if page.is_empty() || index_end >= entries.len() {
        None
    } else {
        Some(
            B64.encode(
                json!({ "uniqueAttributes": arns[index_end] })
                    .to_string()
                    .as_bytes(),
            ),
        )
    };
    Ok((page, next))
}

/// moto's `InvalidToken(AWSError)` — message is prefixed flow-style.
fn invalid_token(message: &str) -> Response {
    json_error(400, "InvalidToken", &format!("Invalid Token: {message}"))
}

// ---------------------------------------------------------------------------
// Validation (moto's DAXResponse validators)
// ---------------------------------------------------------------------------

/// moto's `_validate_arn`: six colon-delimited sections, each missing
/// colon pictured in its own message.
fn validate_arn(arn: &str) -> Result<(), Response> {
    if !arn.starts_with("arn:") {
        return Err(invalid_parameter(&format!(
            "ARNs must start with 'arn:': {arn}"
        )));
    }
    let sections = arn.split(':').count();
    match sections {
        0..=2 => Err(invalid_parameter(&format!(
            "Second colon partition not found: {arn}"
        ))),
        3 => Err(invalid_parameter(&format!(
            "Third colon vendor not found: {arn}"
        ))),
        4 => Err(invalid_parameter(&format!(
            "Fourth colon (region/namespace delimiter) not found: {arn}"
        ))),
        5 => Err(invalid_parameter(&format!(
            "Fifth colon (namespace/relative-id delimiter) not found: {arn}"
        ))),
        _ => Ok(()),
    }
}

/// moto's `_validate_name` regex `^[a-z][a-z0-9-]+[a-z0-9]$` plus the
/// no-double-hyphen rule (no regex crate in rotocore).
fn valid_cluster_name(name: &str) -> bool {
    let chars: Vec<char> = name.chars().collect();
    if chars.len() < 3 {
        return false;
    }
    let first = chars[0].is_ascii_lowercase();
    let last = matches!(chars[chars.len() - 1], 'a'..='z' | '0'..='9');
    let middle = chars[1..chars.len() - 1]
        .iter()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == '-');
    first && last && middle && !name.contains("--")
}

// ---------------------------------------------------------------------------
// Small shared helpers
// ---------------------------------------------------------------------------

fn ok(body: Value) -> Response {
    Response::json(200, serde_json::to_vec(&body).unwrap_or_default())
}

fn cluster_not_found(name: Option<&str>) -> Response {
    json_error(
        400,
        "ClusterNotFoundFault",
        &match name {
            Some(n) => format!("Cluster {n} not found."),
            None => "Cluster not found.".to_string(),
        },
    )
}

fn parameter_group_not_found(name: Option<&str>) -> Response {
    json_error(
        400,
        "ParameterGroupNotFoundFault",
        &match name {
            Some(n) => format!("ParameterGroup {n} not found."),
            None => "ParameterGroup not found.".to_string(),
        },
    )
}

fn subnet_group_not_found(name: &str) -> Response {
    json_error(
        400,
        "SubnetGroupNotFoundFault",
        &format!("SubnetGroup {name} not found."),
    )
}

fn invalid_parameter(message: &str) -> Response {
    json_error(400, "InvalidParameterValueException", message)
}

/// moto's `get_partition` (moto/utilities/utils.py).
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

/// Lowercase hex digits, mirroring moto's `random.get_random_hex`.
fn random_hex(digits: usize) -> String {
    let uuid = Uuid::new_v4().simple().to_string();
    uuid[..digits].to_string()
}

fn as_str<'a>(body: &'a Value, key: &str) -> &'a str {
    body.get(key).and_then(Value::as_str).unwrap_or("")
}

fn as_str_list(body: &Value, key: &str) -> Vec<String> {
    body.get(key)
        .and_then(Value::as_array)
        .map(|list| {
            list.iter()
                .filter_map(Value::as_str)
                .map(String::from)
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
    use std::collections::HashMap as StdHashMap;
    use std::sync::atomic::{AtomicU64, Ordering};

    static ACCOUNTS: AtomicU64 = AtomicU64::new(200_000_000_001);

    /// Every test takes a fresh account so the shared store stays isolated.
    fn next_account() -> u64 {
        ACCOUNTS.fetch_add(1, Ordering::SeqCst)
    }

    fn send(account: u64, region: &str, operation: &str, body: Value) -> (u16, Value) {
        let request = Request {
            service: "dax".to_string(),
            operation: operation.to_string(),
            method: "POST".to_string(),
            path: "/".to_string(),
            query_string: String::new(),
            headers: StdHashMap::new(),
            body: Bytes::from(serde_json::to_vec(&body).unwrap()),
            account,
            region: region.to_string(),
        };
        let resp = dispatch(&request).expect("dax dispatch");
        (
            resp.status,
            serde_json::from_slice(&resp.body).unwrap_or(json!({})),
        )
    }

    fn make_cluster(account: u64, region: &str, name: &str, replication: i64) -> Value {
        let (status, body) = send(
            account,
            region,
            "CreateCluster",
            json!({
                "ClusterName": name,
                "NodeType": "dax.r4.large",
                "ReplicationFactor": replication,
                "IamRoleArn": "arn:aws:iam::123456789012:role/DAXRole",
            }),
        );
        assert_eq!(status, 200, "create_cluster failed: {body}");
        body["Cluster"].clone()
    }

    #[test]
    fn dax_cluster_lifecycle_and_states() {
        let account = next_account();
        let cluster = make_cluster(account, "us-east-1", "dax-lifecycle", 1);
        // A brand-new cluster reports moto's first ManagedState status.
        assert_eq!(cluster["ClusterName"], "dax-lifecycle");
        assert_eq!(cluster["Status"], "creating");
        assert_eq!(
            cluster["ClusterArn"],
            json!(format!(
                "arn:aws:dax:us-east-1:{account}:cache/dax-lifecycle"
            ))
        );
        assert_eq!(cluster["TotalNodes"], 1);
        // The node-bearing shape only arrives once the cluster is available.
        assert!(cluster.get("Nodes").is_none());
        assert_eq!(cluster["ClusterDiscoveryEndpoint"]["Port"], DAX_PORT);

        // Three describes keep it in "creating"; the fourth advances it.
        for expect in ["creating", "creating", "creating"] {
            let (status, body) = send(
                account,
                "us-east-1",
                "DescribeClusters",
                json!({ "ClusterNames": ["dax-lifecycle"] }),
            );
            assert_eq!(status, 200);
            assert_eq!(body["Clusters"][0]["Status"], expect);
        }
        let (status, body) = send(
            account,
            "us-east-1",
            "DescribeClusters",
            json!({ "ClusterNames": ["dax-lifecycle"] }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["Clusters"][0]["Status"], "available");
        assert!(body["Clusters"][0]["Nodes"].as_array().is_some());
        assert!(body["Clusters"][0]["ClusterDiscoveryEndpoint"]["Address"]
            .as_str()
            .is_some());

        // Delete moves the cluster to "deleting" and it disappears once its
        // state machine finishes.
        let (status, body) = send(
            account,
            "us-east-1",
            "DeleteCluster",
            json!({ "ClusterName": "dax-lifecycle" }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["Cluster"]["Status"], "deleting");
        // Each describe ticks the manual state machine; the fourth ticks
        // "deleting" -> "deleted", purges the cluster, and 404s.
        for _ in 0..3 {
            let (status, _) = send(
                account,
                "us-east-1",
                "DescribeClusters",
                json!({ "ClusterNames": ["dax-lifecycle"] }),
            );
            assert_eq!(status, 200);
        }
        let (status, body) = send(
            account,
            "us-east-1",
            "DescribeClusters",
            json!({ "ClusterNames": ["dax-lifecycle"] }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "ClusterNotFoundFault");
    }

    #[test]
    fn dax_create_cluster_validates_arn_and_name() {
        let account = next_account();
        let (status, body) = send(
            account,
            "us-east-1",
            "CreateCluster",
            json!({
                "ClusterName": "dax-bad-name",
                "NodeType": "dax.r4.large",
                "ReplicationFactor": 1,
                "IamRoleArn": "notaarn",
            }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["message"], "ARNs must start with 'arn:': notaarn");

        // Short ARNs report the missing colon, then short names fail the
        // identifier message.
        let (status, body) = send(
            account,
            "us-east-1",
            "CreateCluster",
            json!({
                "ClusterName": "dax-bad-name",
                "NodeType": "dax.r4.large",
                "ReplicationFactor": 1,
                "IamRoleArn": "arn:aws:iam",
            }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["message"], "Third colon vendor not found: arn:aws:iam");

        let (status, body) = send(
            account,
            "us-east-1",
            "CreateCluster",
            json!({
                "ClusterName": "Bad-Name",
                "NodeType": "dax.r4.large",
                "ReplicationFactor": 1,
                "IamRoleArn": "arn:aws:iam::123456789012:role/DAXRole",
            }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["message"], INVALID_CLUSTER_NAME_MSG);

        let (status, body) = send(
            account,
            "us-east-1",
            "CreateCluster",
            json!({
                "ClusterName": "dax-a--b",
                "NodeType": "dax.r4.large",
                "ReplicationFactor": 1,
                "IamRoleArn": "arn:aws:iam::123456789012:role/DAXRole",
            }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["message"], INVALID_CLUSTER_NAME_MSG);
    }

    #[test]
    fn dax_replication_and_nodes() {
        let account = next_account();
        make_cluster(account, "us-west-2", "dax-repl", 1);
        // Advance the manual state machine to "available" (fourth describe
        // ticks over) — moto only renders Nodes on the full shape.
        for _ in 0..3 {
            let (status, body) = send(
                account,
                "us-west-2",
                "DescribeClusters",
                json!({ "ClusterNames": ["dax-repl"] }),
            );
            assert_eq!(status, 200);
            assert_eq!(body["Clusters"][0]["Status"], "creating");
        }
        let (status, body) = send(
            account,
            "us-west-2",
            "DescribeClusters",
            json!({ "ClusterNames": ["dax-repl"] }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["Clusters"][0]["Status"], "available");
        let (status, body) = send(
            account,
            "us-west-2",
            "IncreaseReplicationFactor",
            json!({ "ClusterName": "dax-repl", "NewReplicationFactor": 3 }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["Cluster"]["TotalNodes"], 3);
        let nodes = body["Cluster"]["Nodes"].as_array().unwrap();
        assert_eq!(nodes.len(), 3);
        assert_eq!(nodes[0]["NodeId"], "dax-repl-a");
        assert_eq!(nodes[2]["NodeId"], "dax-repl-c");

        let (status, body) = send(
            account,
            "us-west-2",
            "DecreaseReplicationFactor",
            json!({
                "ClusterName": "dax-repl",
                "NewReplicationFactor": 2,
                "NodeIdsToRemove": ["dax-repl-b"],
            }),
        );
        assert_eq!(status, 200);
        let nodes = body["Cluster"]["Nodes"].as_array().unwrap();
        assert_eq!(nodes.len(), 2);
        assert!(nodes.iter().all(|n| n["NodeId"] != "dax-repl-b"));

        let (status, body) = send(
            account,
            "us-west-2",
            "RebootNode",
            json!({ "ClusterName": "dax-repl", "NodeId": "nope" }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "NodeNotFoundFault");
        assert_eq!(body["message"], "Node nope not found.");

        // A real reboot records an event the filter can find.
        let (status, _body) = send(
            account,
            "us-west-2",
            "RebootNode",
            json!({ "ClusterName": "dax-repl", "NodeId": "dax-repl-a" }),
        );
        assert_eq!(status, 200);
        let (status, body) = send(
            account,
            "us-west-2",
            "DescribeEvents",
            json!({ "SourceType": "NODE" }),
        );
        assert_eq!(status, 200);
        assert!(body["Events"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["SourceName"] == "dax-repl-a"));
    }

    #[test]
    fn dax_tags_follow_the_cluster() {
        let account = next_account();
        let cluster = make_cluster(account, "us-east-1", "dax-tags", 1);
        let arn = cluster["ClusterArn"].as_str().unwrap().to_string();
        let (status, body) = send(
            account,
            "us-east-1",
            "ListTags",
            json!({ "ResourceName": arn.clone() }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["Tags"], json!([]));

        let (status, body) = send(
            account,
            "us-east-1",
            "TagResource",
            json!({ "ResourceName": arn.clone(), "Tags": [{"Key": "env", "Value": "test"}] }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["Tags"], json!([{"Key": "env", "Value": "test"}]));

        let (status, body) = send(
            account,
            "us-east-1",
            "UntagResource",
            json!({ "ResourceName": arn.clone(), "TagKeys": ["env"] }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["Tags"], json!([]));

        // Tags also resolve by bare cluster name.
        let (status, body) = send(
            account,
            "us-east-1",
            "TagResource",
            json!({ "ResourceName": "dax-tags", "Tags": [{"Key": "k", "Value": "v"}] }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["Tags"][0]["Key"], "k");

        // An unknown cluster 404s with moto's name-less message.
        let (status, body) = send(
            account,
            "us-east-1",
            "ListTags",
            json!({ "ResourceName": "arn:aws:dax:us-east-1:123456789012:cache/missing" }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "ClusterNotFoundFault");
        assert_eq!(body["message"], "Cluster not found.");
    }

    #[test]
    fn dax_parameter_groups_behave_like_moto() {
        let account = next_account();
        // The backend seeds moto's default group.
        let (status, body) = send(
            account,
            "us-east-1",
            "DescribeParameters",
            json!({ "ParameterGroupName": DEFAULT_PARAMETER_GROUP }),
        );
        assert_eq!(status, 200);
        let params = body["Parameters"].as_array().unwrap();
        assert_eq!(params.len(), 2);
        assert_eq!(params[0]["ParameterName"], "query-ttl-millis");
        assert_eq!(params[0]["ParameterValue"], "300000");
        assert_eq!(params[0]["Source"], "user");

        // Source filtering matches moto's truthy check.
        let (status, body) = send(
            account,
            "us-east-1",
            "DescribeParameters",
            json!({ "ParameterGroupName": DEFAULT_PARAMETER_GROUP, "Source": "user" }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["Parameters"].as_array().unwrap().len(), 2);

        // Updates change the group's own copy.
        let (status, _) = send(
            account,
            "us-east-1",
            "UpdateParameterGroup",
            json!({
                "ParameterGroupName": DEFAULT_PARAMETER_GROUP,
                "ParameterNameValues": [{"ParameterName": "query-ttl-millis", "ParameterValue": "10"}],
            }),
        );
        assert_eq!(status, 200);
        let (_status, body) = send(
            account,
            "us-east-1",
            "DescribeParameters",
            json!({ "ParameterGroupName": DEFAULT_PARAMETER_GROUP }),
        );
        assert_eq!(body["Parameters"][0]["ParameterValue"], "10");

        // Defaults stay pristine.
        let (status, body) = send(account, "us-east-1", "DescribeDefaultParameters", json!({}));
        assert_eq!(status, 200);
        assert_eq!(body["Parameters"][0]["ParameterValue"], "300000");

        let (status, _body) = send(
            account,
            "us-east-1",
            "DeleteParameterGroup",
            json!({ "ParameterGroupName": DEFAULT_PARAMETER_GROUP }),
        );
        assert_eq!(status, 200);
        // The next store use re-seeds it.
        let (status, _) = send(
            account,
            "us-east-1",
            "DescribeParameters",
            json!({ "ParameterGroupName": DEFAULT_PARAMETER_GROUP }),
        );
        assert_eq!(status, 200);

        let (status, body) = send(
            account,
            "us-east-1",
            "DeleteParameterGroup",
            json!({ "ParameterGroupName": "no-such-group" }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "ParameterGroupNotFoundFault");
        assert_eq!(body["message"], "ParameterGroup no-such-group not found.");

        let (status, body) = send(
            account,
            "us-east-1",
            "CreateParameterGroup",
            json!({ "ParameterGroupName": DEFAULT_PARAMETER_GROUP }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "ParameterGroupAlreadyExistsFault");
        assert_eq!(
            body["message"],
            "ParameterGroup default.dax1.0 already exists."
        );
    }

    #[test]
    fn dax_subnet_groups_and_pagination() {
        let account = next_account();
        for i in 0..3 {
            let (status, _) = send(
                account,
                "us-east-1",
                "CreateSubnetGroup",
                json!({
                    "SubnetGroupName": format!("sg-{i}"),
                    "SubnetIds": ["subnet-00000000"],
                }),
            );
            assert_eq!(status, 200);
        }
        let (status, body) = send(account, "us-east-1", "DescribeSubnetGroups", json!({}));
        assert_eq!(status, 200);
        assert_eq!(body["SubnetGroups"].as_array().unwrap().len(), 3);
        assert_eq!(
            body["SubnetGroups"][0]["Subnets"][0]["SubnetAvailabilityZone"],
            "us-east-1a"
        );

        let (status, body) = send(
            account,
            "us-east-1",
            "DeleteSubnetGroup",
            json!({ "SubnetGroupName": "sg-0" }),
        );
        assert_eq!(status, 200);
        assert_eq!(
            body["DeletionMessage"],
            "SubnetGroup sg-0 has been deleted."
        );

        let (status, body) = send(
            account,
            "us-east-1",
            "DeleteSubnetGroup",
            json!({ "SubnetGroupName": "sg-0" }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "SubnetGroupNotFoundFault");

        // describe_clusters paginates: one per page, resuming by ARN marker.
        for i in 0..3 {
            make_cluster(account, "us-east-1", &format!("dax-page-{i}"), 1);
        }
        let (status, page) = send(
            account,
            "us-east-1",
            "DescribeClusters",
            json!({ "MaxResults": 1 }),
        );
        assert_eq!(status, 200);
        assert_eq!(page["Clusters"].as_array().unwrap().len(), 1);
        let token = page["NextToken"].as_str().expect("next token").to_string();

        let (status, page2) = send(
            account,
            "us-east-1",
            "DescribeClusters",
            json!({ "MaxResults": 1, "NextToken": token }),
        );
        assert_eq!(status, 200);
        let second = page2["Clusters"].as_array().unwrap();
        assert_eq!(second.len(), 1);
        assert_ne!(second[0]["ClusterName"], page["Clusters"][0]["ClusterName"]);
    }

    #[test]
    fn dax_state_is_partitioned() {
        let account = next_account();
        make_cluster(account, "us-east-1", "dax-shared", 1);
        // A different region (and a different account) sees nothing.
        let (status, body) = send(
            account,
            "us-west-2",
            "DescribeClusters",
            json!({ "ClusterNames": ["dax-shared"] }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "ClusterNotFoundFault");
        let (status, _) = send(42, "us-east-1", "DescribeClusters", json!({}));
        assert_eq!(status, 200);
    }
}
