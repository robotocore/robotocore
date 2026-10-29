//! EMR backend (**JSON protocol**, `X-Amz-Target: ElasticMapReduce.<Op>`).
//!
//! The repo's botocore data pins EMR to the JSON wire (jsonVersion 1.1,
//! targetPrefix `ElasticMapReduce`), so both request bodies and responses are
//! JSON, not query-XML. Shapes, error codes and quirks mirror the moto `emr`
//! backend (which the compat suites pin):
//!
//! - Denied-by-default ops return moto's `NotImplemented` document
//!   (`The <snake_op> action has not been implemented`, HTTP 501). The
//!   remaining 501 is UpdateStudio (moto routes absent, suite unpins it).
//!   The former gap ops (ListStudioSessionMappings, StartNotebookExecution,
//!   SetKeepJobFlowAliveWhenNoSteps, SetUnhealthyNodeReplacement,
//!   GetClusterSessionCredentials, GetPersistentAppUIPresignedURL,
//!   GetOnClusterAppUIPresignedURL) are now implemented per the suites.
//! - Unknown clusters answer `InvalidRequestException`/`NoSuchCluster`; a few
//!   moto paths (bare `self.clusters[id]` dict reads) escape as KeyError →
//!   the werkzeug HTML 500 page crash-faithful (SetTerminationProtection,
//!   SetVisibleToAllUsers, TerminateJobFlows, ModifyInstanceGroups).
//! - `Instances.InstanceFleets` on RunJobFlow is ignored — moto only creates
//!   fleets through `AddInstanceFleet`.
//! - State is per-(account, region) via [`crate::store`].

use crate::store;
use crate::{Request, Response};
use serde_json::{json, Map, Value};
use std::collections::HashMap;

// --- embedded release data (moto resources; suites run us-east-1) ------------
const RELEASE_LABELS_US_EAST_1: &str = include_str!("emr_data/release-labels-us-east-1.json");
const INSTANCE_TYPES_EMR_6_10_0: &str = include_str!("emr_data/instance-types-emr-6.10.0.json");

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct Application {
    name: Option<String>,
    version: Option<String>,
    args: Vec<Value>,
    additional_info: Value,
}

#[derive(Debug, Clone)]
struct BootstrapAction {
    name: Option<String>,
    script_path: Option<String>,
    args: Vec<Value>,
    /// Original request value (Name / ScriptBootstrapAction); DescribeJobFlows
    /// renders the raw BootstrapActionConfig back as-is.
    raw: Value,
}

#[derive(Debug, Clone)]
struct Step {
    id: String,
    name: Option<String>,
    action_on_failure: Option<String>,
    hadoop_jar_step: Value,
    created_at: f64,
    /// Monotonic tie-breaker (moto orders by distinct microsecond datetimes).
    seq: u64,
    start_at: Option<f64>,
    end_at: Option<f64>,
    state: String,
}

#[derive(Debug, Clone)]
struct InstanceGroup {
    id: String,
    name: Option<String>,
    role: String,
    instance_type: Option<String>,
    num_instances: i64,
    market: String,
    bid_price: Option<String>,
    /// Attached Auto-Scaling policy (+ moto's injected `status.state=ATTACHED`).
    auto_scaling_policy: Option<Value>,
    ebs_configuration: Option<Value>,
    created_at: f64,
    ready_at: f64,
}

#[derive(Debug, Clone)]
struct Fleet {
    id: String,
    name: String,
    instance_fleet_type: String,
    target_on_demand_capacity: i64,
    target_spot_capacity: i64,
    provisioned_on_demand_capacity: i64,
    provisioned_spot_capacity: i64,
    instance_type_configs: Vec<Value>,
    launch_specifications: Value,
    resize_specifications: Option<Value>,
    cluster_id: String,
}

#[derive(Debug, Clone)]
struct Cluster {
    id: String,
    name: Option<String>,
    log_uri: Option<String>,
    requested_ami_version: Option<String>,
    running_ami_version: Option<String>,
    release_label: Option<String>,
    step_concurrency_level: Option<i64>,
    security_configuration: Option<String>,
    kerberos_attributes: Value,
    auto_scaling_role: Option<String>,
    keep_job_flow_alive_when_no_steps: bool,
    termination_protected: bool,
    visible_to_all_users: bool,
    role: String,
    service_role: Option<String>,
    creation_at: f64,
    start_at: Option<f64>,
    ready_at: Option<f64>,
    end_at: Option<f64>,
    state: Option<String>,
    tags: Vec<(String, String)>,
    applications: Vec<Application>,
    bootstrap_actions: Vec<BootstrapAction>,
    configurations: Vec<Value>,
    steps: Vec<Step>,
    instance_group_ids: Vec<String>,
    master_instance_group_id: Option<String>,
    core_instance_group_id: Option<String>,
    additional_master_security_groups: Vec<String>,
    additional_slave_security_groups: Vec<String>,
    availability_zone: Option<String>,
    ec2_key_name: Option<String>,
    ec2_subnet_id: Option<String>,
    hadoop_version: Option<String>,
    master_security_group: Option<String>,
    service_access_security_group: Option<String>,
    slave_security_group: Option<String>,
}

#[derive(Clone)]
struct SecurityConfiguration {
    name: String,
    body: String,
    created_at: f64,
}

#[derive(Clone)]
struct Studio {
    id: String,
    name: Option<String>,
    auth_mode: Option<String>,
    vpc_id: Option<String>,
    subnet_ids: Vec<Value>,
    service_role: Option<String>,
    user_role: String,
    workspace_security_group_id: Option<String>,
    engine_security_group_id: Option<String>,
    default_s3_location: String,
    description: String,
    created_at: f64,
    tags: Vec<(String, String)>,
}

#[derive(Clone)]
struct StudioSessionMapping {
    studio_id: String,
    identity_id: String,
    identity_name: Option<String>,
    identity_type: String,
    session_policy_arn: Option<String>,
    created_at: f64,
    last_modified_at: f64,
}

#[derive(Clone)]
struct PersistentAppUi {
    id: String,
    target_resource_arn: String,
    created_at: f64,
    last_modified_at: f64,
}

/// Notebook execution record (moto NotebookExecution model subset).
#[derive(Clone)]
struct NotebookExecution {
    id: String,
    editor_id: String,
    execution_engine: Value,
    service_role: String,
    name: String,
    params: String,
    status: String,
    start_at: f64,
    end_at: Option<f64>,
}

#[derive(Default)]
struct EmrState {
    clusters: HashMap<String, Cluster>,
    instance_groups: HashMap<String, InstanceGroup>,
    /// Instances fleets in insertion order (moto's dict listing semantics).
    instance_fleets: Vec<Fleet>,
    security_configurations: HashMap<String, SecurityConfiguration>,
    /// Store mirroring moto's
    /// `{"block_public_access_configuration": …, "block_public_access_configuration_metadata": …}`.
    block_public_access: Option<Value>,
    studios: HashMap<String, Studio>,
    /// Mapping key `{studio}:{identity_type}:{identity_id}` (moto's f-string
    /// key; absent IdentityId shows up as the string "None").
    studio_session_mappings: HashMap<String, StudioSessionMapping>,
    /// Notebook executions {id: record} in moto's create order.
    notebook_executions: HashMap<String, NotebookExecution>,
    managed_scaling_policies: HashMap<String, Value>,
    auto_termination_policies: HashMap<String, Value>,
    persistent_app_uis: HashMap<String, PersistentAppUi>,
}

fn with_state<T>(account: u64, region: &str, f: impl FnOnce(&mut EmrState) -> T) -> T {
    let slot = store::slot::<EmrState>("emr");
    let mut map = slot.write();
    f(map.entry((account, region.to_string())).or_default())
}

// ---------------------------------------------------------------------------
// Params / case-insensitive JSON lookups (moto XFormedDict semantics)
// ---------------------------------------------------------------------------

/// snake_case form of a Pascal/camel identifier (`MasterInstanceType` →
/// `master_instance_type`), matching botocore's `xform_name` (including
/// multi-cap boundaries like `UIPresigned` → `ui_presigned`).
fn xform(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = String::with_capacity(s.len() + 4);
    for (i, &b) in bytes.iter().enumerate() {
        let is_upper = b.is_ascii_uppercase();
        if is_upper && i > 0 {
            let prev = bytes[i - 1];
            let next = bytes.get(i + 1).copied();
            // rule 2: lower/digit run before an upper ("AppUIP…" → before U)
            let rule2 = prev.is_ascii_lowercase() || prev.is_ascii_digit();
            // rule 1: an upper starting a word that continues lowercase,
            // e.g. the "P" in "UIPresigned".
            let rule1 = next.map(|n| n.is_ascii_lowercase()).unwrap_or(false);
            if rule2 || rule1 {
                out.push('_');
            }
        }
        if is_upper {
            out.push(b.to_ascii_lowercase() as char);
        } else {
            out.push(b as char);
        }
    }
    out
}

/// Dotted-path lookup with case-insensitive segments
/// (`Instances.MasterInstanceType`).
fn param<'a>(v: &'a Value, path: &str) -> Option<&'a Value> {
    let mut cur = v;
    for segment in path.split('.') {
        cur = pick(cur, segment)?;
    }
    Some(cur)
}

/// Case-insensitive (xformed) single-key lookup on an object.
fn pick<'a>(v: &'a Value, key: &str) -> Option<&'a Value> {
    let want = xform(key);
    v.as_object()
        .and_then(|m| m.iter().find(|(k, _)| xform(k) == want).map(|(_, val)| val))
}

fn as_str(v: Option<&Value>) -> Option<String> {
    v.and_then(Value::as_str).map(String::from)
}

fn as_i64(v: Option<&Value>) -> Option<i64> {
    match v {
        Some(Value::Number(n)) => n.as_i64().or_else(|| n.as_f64().map(|f| f as i64)),
        Some(Value::String(s)) => s.trim().parse::<i64>().ok(),
        _ => None,
    }
}

fn as_f64(v: Option<&Value>) -> Option<f64> {
    match v {
        Some(Value::Number(n)) => n.as_f64(),
        Some(Value::String(s)) => s.trim().parse::<f64>().ok(),
        _ => None,
    }
}

fn as_vec(v: Option<&Value>) -> Vec<Value> {
    v.and_then(Value::as_array).cloned().unwrap_or_default()
}

fn str_list(v: Option<&Value>) -> Vec<String> {
    as_vec(v)
        .into_iter()
        .filter_map(|x| as_str(Some(&x)))
        .collect()
}

/// moto `_get_bool_param` coercion: bool or the usual strings.
fn bool_param(v: Option<&Value>, default: bool) -> bool {
    match v {
        Some(Value::Bool(b)) => *b,
        Some(Value::String(s)) => matches!(s.as_str(), "true" | "True" | "TRUE"),
        _ => default,
    }
}

// ---------------------------------------------------------------------------
// Errors (exact wire bodies captured from the moto sidecar)
// ---------------------------------------------------------------------------

/// moto's 501 for unimplemented ops.
fn not_implemented(op: &str) -> Response {
    Response::json_status(
        501,
        &json!({
            "__type": "NotImplemented",
            "message": format!("The {} action has not been implemented", xform(op)),
        }),
    )
}

/// Unknown cluster: `InvalidRequestException` with subcode `NoSuchCluster`.
fn invalid_cluster(id: &str) -> Response {
    Response::json_status(
        400,
        &json!({
            "__type": "InvalidRequestException",
            "ErrorCode": "NoSuchCluster",
            "Message": format!("Cluster id '{}' is not valid.", id),
        }),
    )
}

/// moto renders `XxxException(message=…)` keyword form as its fallback text.
fn generic_invalid_request() -> Response {
    Response::json_status(
        400,
        &json!({
            "__type": "InvalidRequestException",
            "Message": "An unspecified service error occurred",
        }),
    )
}

/// ResourceNotFoundException under the InvalidRequestException umbrella.
fn resource_not_found(message: &str) -> Response {
    Response::json_status(
        400,
        &json!({
            "__type": "InvalidRequestException",
            "ErrorCode": "ResourceNotFound",
            "Message": message,
        }),
    )
}

fn validation(message: &str) -> Response {
    // moto/real-AWS JSON 1.1 key ordering: lowercase "message"
    // (moto/core/exceptions.py error_json template).
    Response::json_status(
        400,
        &json!({"__type": "ValidationException", "message": message}),
    )
}

/// moto's KeyError escape (e.g. `self.clusters[unknown_id]`): crash-faithful
/// as moto's werkzeug server — the plain HTML 500 page, no AWS envelope.
fn key_error(_key: &str) -> Response {
    crate::proto::moto_crash_response()
}

fn ok(body: Value) -> Response {
    Response::json_status(200, &body)
}

// ---------------------------------------------------------------------------
// IDs / time
// ---------------------------------------------------------------------------

const ID_CHARS: &[u8] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";

/// Monotonic counter standing in for moto's microsecond-distinct datetimes
/// when ordering steps newest-first.
fn next_seq() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: once_cell::sync::Lazy<AtomicU64> = once_cell::sync::Lazy::new(|| AtomicU64::new(0));
    SEQ.fetch_add(1, Ordering::Relaxed)
}

fn random_id() -> String {
    let bytes = *uuid::Uuid::new_v4().as_bytes();
    (0..13)
        .map(|i| ID_CHARS[(bytes[i % bytes.len()] as usize) % ID_CHARS.len()] as char)
        .collect()
}

fn random_cluster_id() -> String {
    format!("j-{}", random_id())
}

fn random_instance_group_id() -> String {
    format!("i-{}", random_id())
}

fn random_instance_fleet_id() -> String {
    format!("if-{}", random_id())
}

fn random_studio_id() -> String {
    format!("es-{}", random_id())
}

fn random_step_id() -> String {
    format!("s-{}", random_id())
}

/// Epoch-seconds float with millisecond precision (moto's JSON timestamps).
fn now_ts() -> f64 {
    (chrono::Utc::now().timestamp_millis() as f64) / 1000.0
}

fn cluster_arn(account: u64, region: &str, id: &str) -> String {
    format!("arn:aws:elasticmapreduce:{region}:{account}:cluster/{id}")
}

// ---------------------------------------------------------------------------
// JSON rendering helpers (moto drops None-valued members)
// ---------------------------------------------------------------------------

fn put_opt_str(m: &mut Map<String, Value>, key: &str, v: Option<&String>) {
    if let Some(v) = v {
        m.insert(key.to_string(), json!(v));
    }
}

fn put_opt_ts(m: &mut Map<String, Value>, key: &str, v: Option<f64>) {
    if let Some(v) = v {
        m.insert(key.to_string(), json!(v));
    }
}

fn put_opt_num(m: &mut Map<String, Value>, key: &str, v: Option<i64>) {
    if let Some(v) = v {
        m.insert(key.to_string(), json!(v));
    }
}

// ---------------------------------------------------------------------------
// Cluster machinery (moto Cluster semantics)
// ---------------------------------------------------------------------------

/// Cluster.status
fn cluster_status_json(c: &Cluster) -> Value {
    let mut obj = Map::new();
    obj.insert("State".into(), json!(c.state.clone().unwrap_or_default()));
    // moto: StateChangeReason {"Message": getattr(..., None)} → unset dropped.
    obj.insert("StateChangeReason".into(), json!({}));
    let mut timeline = Map::new();
    timeline.insert("CreationDateTime".into(), json!(c.creation_at));
    put_opt_ts(&mut timeline, "EndDateTime", c.end_at);
    put_opt_ts(&mut timeline, "ReadyDateTime", c.ready_at);
    obj.insert("Timeline".into(), Value::Object(timeline));
    Value::Object(obj)
}

/// ClusterEc2InstanceAttributes — the Ec2InstanceAttributes member.
fn cluster_ec2_attrs_json(c: &Cluster) -> Value {
    let mut obj = Map::new();
    put_opt_str(
        &mut obj,
        "Ec2AvailabilityZone",
        c.availability_zone.as_ref(),
    );
    put_opt_str(&mut obj, "Ec2KeyName", c.ec2_key_name.as_ref());
    put_opt_str(&mut obj, "Ec2SubnetId", c.ec2_subnet_id.as_ref());
    obj.insert("IamInstanceProfile".into(), json!(c.role));
    put_opt_str(
        &mut obj,
        "EmrManagedMasterSecurityGroup",
        c.master_security_group.as_ref(),
    );
    put_opt_str(
        &mut obj,
        "EmrManagedSlaveSecurityGroup",
        c.slave_security_group.as_ref(),
    );
    put_opt_str(
        &mut obj,
        "ServiceAccessSecurityGroup",
        c.service_access_security_group.as_ref(),
    );
    obj.insert(
        "AdditionalMasterSecurityGroups".into(),
        json!(c.additional_master_security_groups),
    );
    obj.insert(
        "AdditionalSlaveSecurityGroups".into(),
        json!(c.additional_slave_security_groups),
    );
    Value::Object(obj)
}

fn applications_json(apps: &[Application]) -> Vec<Value> {
    apps.iter()
        .map(|a| {
            let mut app = Map::new();
            put_opt_str(&mut app, "Name", a.name.as_ref());
            put_opt_str(&mut app, "Version", a.version.as_ref());
            app.insert("Args".into(), json!(a.args));
            app.insert("AdditionalInfo".into(), json!(a.additional_info));
            Value::Object(app)
        })
        .collect()
}

impl Cluster {
    fn arn(&self, account: u64, region: &str) -> String {
        cluster_arn(account, region, &self.id)
    }

    /// DescribeCluster Cluster member (moto's Cluster serialization).
    fn to_json(&self, account: u64, region: &str) -> Value {
        let mut obj = Map::new();
        obj.insert("Id".into(), json!(self.id));
        put_opt_str(&mut obj, "Name", self.name.as_ref());
        obj.insert("Status".into(), cluster_status_json(self));
        obj.insert("Ec2InstanceAttributes".into(), cluster_ec2_attrs_json(self));
        put_opt_str(&mut obj, "LogUri", self.log_uri.as_ref());
        put_opt_str(
            &mut obj,
            "RequestedAmiVersion",
            self.requested_ami_version.as_ref(),
        );
        put_opt_str(
            &mut obj,
            "RunningAmiVersion",
            self.running_ami_version.as_ref(),
        );
        put_opt_str(&mut obj, "ReleaseLabel", self.release_label.as_ref());
        obj.insert(
            "AutoTerminate".into(),
            json!(!self.keep_job_flow_alive_when_no_steps),
        );
        obj.insert(
            "TerminationProtected".into(),
            json!(self.termination_protected),
        );
        obj.insert("VisibleToAllUsers".into(), json!(self.visible_to_all_users));
        obj.insert(
            "Applications".into(),
            json!(applications_json(&self.applications)),
        );
        obj.insert(
            "Tags".into(),
            json!(self
                .tags
                .iter()
                .map(|(k, v)| json!({"Key": k, "Value": v}))
                .collect::<Vec<_>>()),
        );
        put_opt_str(&mut obj, "ServiceRole", self.service_role.as_ref());
        obj.insert("NormalizedInstanceHours".into(), json!(0));
        // moto's hardcoded fallback (no EC2 instance bookkeeping wired)
        obj.insert(
            "MasterPublicDnsName".into(),
            json!("ec2-184-0-0-1.us-west-1.compute.amazonaws.com"),
        );
        obj.insert("Configurations".into(), json!(self.configurations));
        put_opt_str(
            &mut obj,
            "SecurityConfiguration",
            self.security_configuration.as_ref(),
        );
        obj.insert("KerberosAttributes".into(), json!(self.kerberos_attributes));
        obj.insert("ClusterArn".into(), json!(self.arn(account, region)));
        put_opt_num(
            &mut obj,
            "StepConcurrencyLevel",
            self.step_concurrency_level,
        );
        Value::Object(obj)
    }

    /// ClusterSummary element for ListClusters.
    fn summary_json(&self, account: u64, region: &str) -> Value {
        json!({
            "Id": self.id,
            "Name": self.name,
            "Status": cluster_status_json(self),
            "NormalizedInstanceHours": 0,
            "ClusterArn": self.arn(account, region),
        })
    }

    /// JobFlowDetail element for DescribeJobFlows.
    fn job_flow_json(&self, state: &EmrState) -> Value {
        let mut obj = Map::new();
        obj.insert("JobFlowId".into(), json!(self.id));
        put_opt_str(&mut obj, "Name", self.name.as_ref());
        put_opt_str(&mut obj, "LogUri", self.log_uri.as_ref());
        put_opt_str(&mut obj, "AmiVersion", self.running_ami_version.as_ref());
        let mut exec = Map::new();
        exec.insert(
            "State".into(),
            json!(self.state.clone().unwrap_or_default()),
        );
        exec.insert("CreationDateTime".into(), json!(self.creation_at));
        put_opt_ts(&mut exec, "StartDateTime", self.start_at);
        put_opt_ts(&mut exec, "ReadyDateTime", self.ready_at);
        put_opt_ts(&mut exec, "EndDateTime", self.end_at);
        obj.insert("ExecutionStatusDetail".into(), Value::Object(exec));
        obj.insert("Instances".into(), job_flow_instances_json(self, state));
        let steps = self
            .steps
            .iter()
            .map(|s| {
                let mut cfg = Map::new();
                put_opt_str(&mut cfg, "Name", s.name.as_ref());
                put_opt_str(&mut cfg, "ActionOnFailure", s.action_on_failure.as_ref());
                cfg.insert("HadoopJarStep".into(), s.hadoop_jar_step.clone());
                let mut detail = Map::new();
                detail.insert("State".into(), json!(s.state.clone()));
                detail.insert("CreationDateTime".into(), json!(s.created_at));
                put_opt_ts(&mut detail, "StartDateTime", s.start_at);
                put_opt_ts(&mut detail, "EndDateTime", s.end_at);
                json!({
                    "StepConfig": Value::Object(cfg),
                    "ExecutionStatusDetail": Value::Object(detail),
                })
            })
            .collect::<Vec<_>>();
        obj.insert("Steps".into(), json!(steps));
        let bas = self
            .bootstrap_actions
            .iter()
            .map(|a| json!({"BootstrapActionConfig": a.raw}))
            .collect::<Vec<_>>();
        obj.insert("BootstrapActions".into(), json!(bas));
        obj.insert("SupportedProducts".into(), json!([]));
        obj.insert("VisibleToAllUsers".into(), json!(self.visible_to_all_users));
        obj.insert("JobFlowRole".into(), json!(self.role));
        put_opt_str(&mut obj, "ServiceRole", self.service_role.as_ref());
        put_opt_str(&mut obj, "AutoScalingRole", self.auto_scaling_role.as_ref());
        Value::Object(obj)
    }
}

/// JobFlowInstancesDetail.
fn job_flow_instances_json(c: &Cluster, state: &EmrState) -> Value {
    let group_type = |id: &Option<String>| {
        id.as_ref()
            .and_then(|i| state.instance_groups.get(i))
            .and_then(|g| g.instance_type.clone())
    };
    let mut obj = Map::new();
    put_opt_str(
        &mut obj,
        "MasterInstanceType",
        group_type(&c.master_instance_group_id).as_ref(),
    );
    obj.insert(
        "MasterPublicDnsName".into(),
        json!("ec2-184-0-0-1.us-west-1.compute.amazonaws.com"),
    );
    put_opt_str(
        &mut obj,
        "SlaveInstanceType",
        group_type(&c.core_instance_group_id).as_ref(),
    );
    let count: i64 = c
        .instance_group_ids
        .iter()
        .filter_map(|id| state.instance_groups.get(id))
        .map(|g| g.num_instances)
        .sum();
    obj.insert("InstanceCount".into(), json!(count));
    obj.insert(
        "InstanceGroups".into(),
        json!(job_flow_groups_json(c, state)),
    );
    obj.insert("NormalizedInstanceHours".into(), json!(0));
    obj.insert(
        "Placement".into(),
        json!({"AvailabilityZone": c.availability_zone}),
    );
    obj.insert(
        "KeepJobFlowAliveWhenNoSteps".into(),
        json!(c.keep_job_flow_alive_when_no_steps),
    );
    obj.insert(
        "TerminationProtected".into(),
        json!(c.termination_protected),
    );
    put_opt_str(&mut obj, "Ec2KeyName", c.ec2_key_name.as_ref());
    put_opt_str(&mut obj, "Ec2SubnetId", c.ec2_subnet_id.as_ref());
    put_opt_str(&mut obj, "HadoopVersion", c.hadoop_version.as_ref());
    Value::Object(obj)
}

/// Legacy JobFlowInstanceGroup elements (DescribeJobFlows Instances list).
fn job_flow_groups_json(c: &Cluster, state: &EmrState) -> Vec<Value> {
    c.instance_group_ids
        .iter()
        .filter_map(|id| state.instance_groups.get(id))
        .map(|g| {
            json!({
                "InstanceGroupId": g.id,
                "Name": g.name,
                "Market": g.market,
                "InstanceRole": g.role,
                "InstanceType": g.instance_type,
                "InstanceRequestCount": g.num_instances,
                "InstanceRunningCount": g.num_instances,
                "State": "RUNNING",
                "CreationDateTime": c.creation_at,
                "StartDateTime": c.creation_at,
                "ReadyDateTime": c.ready_at.unwrap_or(c.creation_at),
            })
        })
        .collect()
}

/// InstanceGroupList elements — groups sorted by id like moto (ties broken by
/// insertion order for equal ids, which cannot happen with unique ids).
fn list_instance_groups_json(state: &EmrState, cluster: &Cluster) -> Vec<Value> {
    let mut groups: Vec<&InstanceGroup> = cluster
        .instance_group_ids
        .iter()
        .filter_map(|id| state.instance_groups.get(id))
        .collect();
    groups.sort_by(|a, b| a.id.cmp(&b.id));
    groups
        .iter()
        .map(|g| {
            let mut obj = Map::new();
            obj.insert("Id".into(), json!(g.id));
            put_opt_str(&mut obj, "Name", g.name.as_ref());
            obj.insert("Market".into(), json!(g.market));
            obj.insert("InstanceGroupType".into(), json!(g.role));
            put_opt_str(&mut obj, "InstanceType", g.instance_type.as_ref());
            obj.insert("RequestedInstanceCount".into(), json!(g.num_instances));
            obj.insert("RunningInstanceCount".into(), json!(g.num_instances));
            let mut status = Map::new();
            status.insert("State".into(), json!("RUNNING"));
            status.insert("StateChangeReason".into(), json!({"Code": "USER_REQUEST"}));
            let mut timeline = Map::new();
            timeline.insert("CreationDateTime".into(), json!(g.created_at));
            timeline.insert("ReadyDateTime".into(), json!(g.ready_at));
            status.insert("Timeline".into(), Value::Object(timeline));
            obj.insert("Status".into(), Value::Object(status));
            if let Some(policy) = &g.auto_scaling_policy {
                obj.insert("AutoScalingPolicy".into(), policy_snake_to_pascal(policy));
            }
            put_opt_str(&mut obj, "BidPrice", g.bid_price.as_ref());
            obj.insert(
                "EbsBlockDevices".into(),
                json!(ebs_block_devices_json(&g.ebs_configuration)),
            );
            Value::Object(obj)
        })
        .collect()
}

/// moto InstanceGroup.ebs_block_devices — expands ebs_block_device_configs.
fn ebs_block_devices_json(ebs_configuration: &Option<Value>) -> Vec<Value> {
    let Some(config) = ebs_configuration else {
        return vec![];
    };
    as_vec(pick(config, "ebs_block_device_configs"))
        .into_iter()
        .flat_map(|entry| {
            let volumes_per = as_i64(pick(&entry, "volumes_per_instance")).unwrap_or(0);
            let spec = pick(&entry, "volume_specification");
            (0..volumes_per_max(volumes_per))
                .map(|i| {
                    json!({
                        "VolumeSpecification": {
                            "VolumeType": spec.and_then(|s| pick(s, "volume_type")).cloned().unwrap_or(Value::Null),
                            "Iops": spec.and_then(|s| pick(s, "iops")).cloned().unwrap_or(Value::Null),
                            "SizeInGB": spec.and_then(|s| pick(s, "size_in_gb")).cloned().unwrap_or(Value::Null),
                        },
                        "Device": format!("/dev/sd{i}"),
                    })
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

fn volumes_per_max(v: i64) -> i64 {
    v.clamp(0, 32)
}

fn terminate_cluster(c: &mut Cluster) {
    c.state = Some("TERMINATING".into());
    c.end_at = Some(now_ts());
    c.state = Some("TERMINATED".into());
}

// ---
// Family B: steps, instance groups, fleets, bootstrap actions, scaling policies
// ---

/// StepSummary element (DescribeStep / ListSteps members).
fn step_summary_json(s: &Step) -> Value {
    let config = step_config_json(s);
    let mut status = Map::new();
    status.insert("State".into(), json!(s.state));
    // moto: StateChangeReason {"Message": getattr(..., None)} → unset dropped.
    status.insert("StateChangeReason".into(), json!({}));
    let mut timeline = Map::new();
    timeline.insert("CreationDateTime".into(), json!(s.created_at));
    put_opt_ts(&mut timeline, "EndDateTime", s.end_at);
    put_opt_ts(&mut timeline, "StartDateTime", s.start_at);
    status.insert("Timeline".into(), Value::Object(timeline));
    let mut obj = Map::new();
    put_opt_str(&mut obj, "Id", Some(&s.id));
    put_opt_str(&mut obj, "Name", s.name.as_ref());
    obj.insert("Config".into(), config);
    put_opt_str(&mut obj, "ActionOnFailure", s.action_on_failure.as_ref());
    obj.insert("Status".into(), Value::Object(status));
    Value::Object(obj)
}

/// HadoopStepConfig members for a step (moto Step.config).
fn step_config_json(s: &Step) -> Value {
    let jar = as_str(pick(&s.hadoop_jar_step, "jar")).unwrap_or_default();
    let args = as_vec(pick(&s.hadoop_jar_step, "args"));
    // moto: {p["Key"]: p["Value"] for p in request properties}
    let properties = as_vec(pick(&s.hadoop_jar_step, "properties"))
        .iter()
        .filter_map(|p| {
            Some((
                as_str(pick(p, "key"))?,
                pick(p, "value").cloned().unwrap_or(Value::Null),
            ))
        })
        .collect::<Vec<(String, Value)>>();
    let mut props = Map::new();
    for (k, v) in properties {
        props.insert(k, v);
    }
    json!({"Jar": jar, "Properties": Value::Object(props), "Args": args})
}

/// Insert policy snake-xformed with moto's injected `status.state=ATTACHED`.
fn attach_auto_scaling_policy(policy: Option<&Value>, cluster_id: &str) -> Option<Value> {
    let stored = policy.map(pascal_to_snake_tree).filter(|p| !p.is_null())?;
    let stored = match stored {
        Value::Object(mut obj) => {
            obj.insert("status".into(), json!({"state": "ATTACHED"}));
            Value::Object(obj)
        }
        other => other,
    };
    // moto: swap the ${emr.clusterId} placeholder in alarm dimensions.
    Some(swap_cluster_placeholder(stored, cluster_id))
}

fn swap_cluster_placeholder(mut v: Value, cluster_id: &str) -> Value {
    let needle = "${emr.clusterId}".to_string();
    match &mut v {
        Value::Array(items) => {
            for item in items {
                let cloned = item.clone();
                let swapped = swap_cluster_placeholder(cloned, cluster_id);
                *item = swapped;
            }
        }
        Value::Object(obj) => {
            for (_k, val) in obj.iter_mut() {
                let cloned = val.clone();
                let swapped = swap_cluster_placeholder(cloned, cluster_id);
                *val = swapped;
            }
        }
        Value::String(s) if *s == needle => {
            // dimension values carrying the placeholder become the id
            return Value::String(cluster_id.to_string());
        }
        _ => {}
    }
    v
}

/// recursive camel→snake key transform (moto CamelToUnderscoresWalker).
fn pascal_to_snake_tree(v: &Value) -> Value {
    match v {
        Value::Array(items) => Value::Array(items.iter().map(pascal_to_snake_tree).collect()),
        Value::Object(map) => {
            let mut out = Map::new();
            for (k, val) in map {
                out.insert(pascal_to_snake(k), pascal_to_snake_tree(val));
            }
            Value::Object(out)
        }
        other => other.clone(),
    }
}

/// `SimpleScalingPolicyConfiguration` → `simple_scaling_policy_configuration`.
fn pascal_to_snake(s: &str) -> String {
    xform(s)
}

/// Render a stored (snake-keyed) Auto-Scaling policy as PascalCase members,
/// the way moto's shape-driven serializer does.
fn policy_snake_to_pascal(v: &Value) -> Value {
    match v {
        Value::Array(items) => Value::Array(items.iter().map(policy_snake_to_pascal).collect()),
        Value::Object(map) => {
            let mut out = Map::new();
            for (k, val) in map {
                out.insert(snake_to_pascal(k), policy_snake_to_pascal(val));
            }
            Value::Object(out)
        }
        other => other.clone(),
    }
}

/// `simple_scaling_policy_configuration` → `SimpleScalingPolicyConfiguration`.
fn snake_to_pascal(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for word in s.split('_') {
        if word.is_empty() {
            continue;
        }
        let mut cs = word.chars();
        if let Some(first) = cs.next() {
            out.extend(first.to_uppercase());
            out.push_str(cs.as_str());
        }
    }
    out
}

// ---------------------------------------------------------------------------
// dispatch
// ---------------------------------------------------------------------------

fn body_json(req: &Request) -> Value {
    serde_json::from_slice(&req.body).unwrap_or_else(|_| json!({}))
}

/// Operation name, falling back to the X-Amz-Target suffix.
fn operation_of(req: &Request) -> String {
    if !req.operation.is_empty() {
        return req.operation.clone();
    }
    req.headers
        .get("x-amz-target")
        .and_then(|t| t.rsplit('.').next())
        .unwrap_or("")
        .to_string()
}

type OpResult = Result<Value, Response>;

pub fn dispatch(req: &Request) -> Response {
    let op = operation_of(req);
    let body = body_json(req);
    let account = req.account;
    let region = req.region.clone();
    let result: OpResult = with_state(req.account, &req.region, |st| match op.as_str() {
        "RunJobFlow" => run_job_flow(st, account, &region, &body),
        "DescribeCluster" => describe_cluster(st, account, &region, &body),
        "ListClusters" => list_clusters(st, account, &region, &body),
        "TerminateJobFlows" => terminate_job_flows(st, &body),
        "SetTerminationProtection" => set_termination_protection(st, &body),
        "SetVisibleToAllUsers" => set_visible_to_all_users(st, &body),
        "AddTags" => add_tags(st, &body),
        "RemoveTags" => remove_tags(st, &body),
        "ModifyCluster" => modify_cluster(st, &body),
        "DescribeJobFlows" => describe_job_flows(st, &body),
        "AddJobFlowSteps" => add_job_flow_steps(st, &body),
        "ListSteps" => list_steps(st, &body),
        "DescribeStep" => describe_step(st, &body),
        "CancelSteps" => cancel_steps(st, &body),
        "AddInstanceGroups" => add_instance_groups(st, &body),
        "ListInstanceGroups" => list_instance_groups(st, &body),
        "ModifyInstanceGroups" => modify_instance_groups(st, &body),
        "ListInstances" => list_instances(st, &body),
        "ListBootstrapActions" => list_bootstrap_actions(st, &body),
        "AddInstanceFleet" => add_instance_fleet(st, account, &region, &body),
        "ModifyInstanceFleet" => modify_instance_fleet(st, &body),
        "ListInstanceFleets" => list_instance_fleets(st, account, &region, &body),
        "PutAutoScalingPolicy" => put_auto_scaling_policy(st, account, &region, &body),
        "RemoveAutoScalingPolicy" => remove_auto_scaling_policy(st, &body),
        "CreateSecurityConfiguration" => create_security_configuration(st, &body),
        "DescribeSecurityConfiguration" => describe_security_configuration(st, &body),
        "DeleteSecurityConfiguration" => delete_security_configuration(st, &body),
        "ListSecurityConfigurations" => list_security_configurations(st, &body),
        "PutBlockPublicAccessConfiguration" => {
            put_block_public_access_configuration(st, account, &body)
        }
        "GetBlockPublicAccessConfiguration" => get_block_public_access_configuration(st, &body),
        "CreateStudio" => create_studio(st, &region, &body),
        "DescribeStudio" => describe_studio(st, account, &region, &body),
        "DeleteStudio" => delete_studio(st, &body),
        "ListStudios" => list_studios(st, &region, &body),
        "CreateStudioSessionMapping" => create_studio_session_mapping(st, &body),
        "GetStudioSessionMapping" => get_studio_session_mapping(st, &body),
        "UpdateStudioSessionMapping" => update_studio_session_mapping(st, &body),
        "DeleteStudioSessionMapping" => delete_studio_session_mapping(st, &body),
        "ListStudioSessionMappings" => list_studio_session_mappings(st, &body),
        "DescribeNotebookExecution" => describe_notebook_execution(st, &body),
        "StopNotebookExecution" => stop_notebook_execution(st, &body),
        "ListNotebookExecutions" => list_notebook_executions(st, &body),
        "StartNotebookExecution" => start_notebook_execution(st, account, &region, &body),
        "SetKeepJobFlowAliveWhenNoSteps" => set_keep_job_flow_alive_when_no_steps(st, &body),
        "SetUnhealthyNodeReplacement" => set_unhealthy_node_replacement(&body),
        "GetClusterSessionCredentials" => get_cluster_session_credentials(&body),
        "CreatePersistentAppUI" => create_persistent_app_ui(st, account, &region, &body),
        "DescribePersistentAppUI" => describe_persistent_app_ui(st, account, &region, &body),
        "GetPersistentAppUIPresignedURL" => get_persistent_app_ui_presigned_url(st, &region, &body),
        "GetOnClusterAppUIPresignedURL" => get_on_cluster_app_ui_presigned_url(&region, &body),
        "DescribeReleaseLabel" => describe_release_label(st, &body),
        "ListReleaseLabels" => list_release_labels(),
        "ListSupportedInstanceTypes" => list_supported_instance_types(&body),
        "PutManagedScalingPolicy" => put_managed_scaling_policy(st, &body),
        "GetManagedScalingPolicy" => get_managed_scaling_policy(st, &body),
        "RemoveManagedScalingPolicy" => remove_managed_scaling_policy(st, &body),
        "PutAutoTerminationPolicy" => put_auto_termination_policy(st, &body),
        "GetAutoTerminationPolicy" => get_auto_termination_policy(st, &body),
        "RemoveAutoTerminationPolicy" => remove_auto_termination_policy(st, &body),
        _ => Err(not_implemented(&op)),
    });
    match result {
        Ok(v) => ok(v),
        Err(resp) => resp,
    }
}

// ---------------------------------------------------------------------------
// Family A handlers
// ---------------------------------------------------------------------------

/// RunJobFlow — moto responses.run_job_flow + Cluster.__init__ ordering.
fn run_job_flow(state: &mut EmrState, account: u64, region: &str, body: &Value) -> OpResult {
    let name = as_str(param(body, "Name"));
    let log_uri = as_str(param(body, "LogUri"));
    let job_flow_role = as_str(param(body, "JobFlowRole"));
    let service_role = as_str(param(body, "ServiceRole"));
    let auto_scaling_role = as_str(param(body, "AutoScalingRole"));

    let release_label = as_str(param(body, "ReleaseLabel"));
    let ami_version = as_str(param(body, "AmiVersion"));
    if release_label.is_some() && ami_version.is_some() {
        return Err(validation(&format!(
            "Only one AMI version and release label may be specified. Provided AMI: {}, release label: {}.",
            ami_version.unwrap_or_default(),
            release_label.unwrap_or_default()
        )));
    }
    let (requested_ami, running_ami) = match &release_label {
        Some(_) => (None, None),
        None => (
            ami_version.clone(),
            Some(ami_version.clone().unwrap_or_else(|| "1.0.0".into())),
        ),
    };

    let step_concurrency = as_i64(param(body, "StepConcurrencyLevel"));
    let security_configuration = as_str(param(body, "SecurityConfiguration"));
    let kerberos_attributes = param(body, "KerberosAttributes")
        .cloned()
        .unwrap_or(json!({}));

    let keep_alive = bool_param(param(body, "Instances.KeepJobFlowAliveWhenNoSteps"), false);
    let termination_protected = bool_param(param(body, "Instances.TerminationProtected"), false);
    let availability_zone = as_str(param(body, "Instances.Placement.AvailabilityZone"))
        .unwrap_or_else(|| format!("{region}a"));
    let additional_master = str_list(param(body, "Instances.AdditionalMasterSecurityGroups"));
    let additional_slave = str_list(param(body, "Instances.AdditionalSlaveSecurityGroups"));
    let ec2_key_name = as_str(param(body, "Instances.Ec2KeyName"));
    let ec2_subnet_id = as_str(param(body, "Instances.Ec2SubnetId"));
    let hadoop_version = as_str(param(body, "Instances.HadoopVersion"));
    let master_sg = as_str(param(body, "Instances.EmrManagedMasterSecurityGroup"));
    let slave_sg = as_str(param(body, "Instances.EmrManagedSlaveSecurityGroup"));
    let service_sg = as_str(param(body, "Instances.ServiceAccessSecurityGroup"));
    let master_instance_type = as_str(param(body, "Instances.MasterInstanceType"));
    let slave_instance_type = as_str(param(body, "Instances.SlaveInstanceType"));
    let instance_count = as_i64(param(body, "Instances.InstanceCount")).unwrap_or(1);

    let id = random_cluster_id();
    let creation_at = now_ts();
    let mut cluster = Cluster {
        id: id.clone(),
        name,
        log_uri,
        requested_ami_version: requested_ami,
        running_ami_version: running_ami,
        release_label: release_label.clone(),
        step_concurrency_level: step_concurrency.or(Some(1)),
        security_configuration,
        kerberos_attributes,
        auto_scaling_role,
        keep_job_flow_alive_when_no_steps: keep_alive,
        termination_protected,
        visible_to_all_users: bool_param(param(body, "VisibleToAllUsers"), false),
        role: job_flow_role.unwrap_or_else(|| "EMRJobflowDefault".into()),
        service_role,
        creation_at,
        start_at: None,
        ready_at: None,
        end_at: None,
        state: None,
        tags: Vec::new(),
        applications: Vec::new(),
        bootstrap_actions: Vec::new(),
        configurations: as_vec(param(body, "Configurations")),
        steps: Vec::new(),
        instance_group_ids: Vec::new(),
        master_instance_group_id: None,
        core_instance_group_id: None,
        additional_master_security_groups: additional_master,
        additional_slave_security_groups: additional_slave,
        availability_zone: Some(availability_zone),
        ec2_key_name,
        ec2_subnet_id,
        hadoop_version,
        master_security_group: master_sg,
        service_access_security_group: service_sg,
        slave_security_group: slave_sg,
    };

    // moto __init__ follows: tags → add_steps → bootstrap actions → state
    // machine (STARTING → BOOTSTRAPPING → WAITING → maybe auto-terminate).
    let tags_in: Vec<(String, String)> = as_vec(param(body, "Tags"))
        .iter()
        .filter_map(|t| Some((as_str(pick(t, "key"))?, as_str(pick(t, "value"))?)))
        .collect();
    cluster.tags.extend(tags_in);

    // AddSteps: first of an empty list is RUNNING, later ones PENDING.
    for step in as_vec(param(body, "Steps")).iter() {
        let state_name = if cluster.steps.is_empty() {
            "RUNNING"
        } else {
            "PENDING"
        };
        cluster.steps.push(Step {
            id: random_step_id(),
            name: as_str(pick(step, "name")),
            action_on_failure: as_str(pick(step, "action_on_failure")),
            hadoop_jar_step: pick(step, "hadoop_jar_step").cloned().unwrap_or(json!({})),
            created_at: now_ts(),
            seq: next_seq(),
            start_at: None,
            end_at: None,
            state: state_name.into(),
        });
    }

    for action in as_vec(param(body, "BootstrapActions")) {
        let name = as_str(pick(&action, "name"));
        let script = pick(&action, "script_bootstrap_action");
        let script_path = script.and_then(|s| as_str(pick(s, "path")));
        let args = script.map(|s| as_vec(pick(s, "args"))).unwrap_or_default();
        cluster.bootstrap_actions.push(BootstrapAction {
            name,
            script_path,
            args,
            raw: action,
        });
    }

    cluster.start_at = Some(creation_at);
    cluster.ready_at = Some(creation_at);
    cluster.state = Some("WAITING".into());
    if cluster.steps.is_empty() && !cluster.keep_job_flow_alive_when_no_steps {
        cluster.state = Some("TERMINATING".into());
        cluster.end_at = Some(creation_at);
        cluster.state = Some("TERMINATED".into());
    }
    if let Some(first) = cluster.steps.first_mut() {
        first.start_at = Some(creation_at);
    }

    // instance groups: master/core types auto-create groups (moto adds groups
    // before the state machine; the visible timeline uses cluster timestamps).
    if let Some(master_type) = master_instance_type {
        let group = InstanceGroup {
            id: random_instance_group_id(),
            name: Some("master".into()),
            role: "MASTER".into(),
            instance_type: Some(master_type),
            num_instances: 1,
            market: "ON_DEMAND".into(),
            bid_price: None,
            auto_scaling_policy: None,
            ebs_configuration: None,
            created_at: creation_at,
            ready_at: creation_at,
        };
        cluster.instance_group_ids.push(group.id.clone());
        cluster.master_instance_group_id = Some(group.id.clone());
        state.instance_groups.insert(group.id.clone(), group);
    }
    if let Some(slave_type) = slave_instance_type {
        let group = InstanceGroup {
            id: random_instance_group_id(),
            name: Some("slave".into()),
            role: "CORE".into(),
            instance_type: Some(slave_type),
            num_instances: instance_count - 1,
            market: "ON_DEMAND".into(),
            bid_price: None,
            auto_scaling_policy: None,
            ebs_configuration: None,
            created_at: creation_at,
            ready_at: creation_at,
        };
        cluster.instance_group_ids.push(group.id.clone());
        cluster.core_instance_group_id = Some(group.id.clone());
        state.instance_groups.insert(group.id.clone(), group);
    }

    // Applications: moto's default Hadoop literal loses Name/Version while
    // caller-supplied Applications keep them (xformed-key lookups).
    let apps_in = as_vec(param(body, "Applications"));
    cluster.applications = if apps_in.is_empty() {
        vec![Application {
            name: None,
            version: None,
            args: vec![],
            additional_info: json!({}),
        }]
    } else {
        apps_in
            .iter()
            .map(|a| Application {
                name: as_str(pick(a, "name")),
                version: as_str(pick(a, "version")),
                args: as_vec(pick(a, "args")),
                additional_info: pick(a, "additional_info").cloned().unwrap_or(json!({})),
            })
            .collect()
    };

    let mut master_us: u8 = 0;
    if cluster.master_instance_group_id.is_some() {
        master_us += 1;
    }
    let _ = master_us;

    state.clusters.insert(cluster.id.clone(), cluster);

    let created = state.clusters.get(&id).unwrap();
    Ok(json!({
        "JobFlowId": id,
        "ClusterArn": created.arn(account, region),
    }))
}

/// DescribeCluster — `{"Cluster": …}` or NoSuchCluster.
fn describe_cluster(state: &mut EmrState, account: u64, region: &str, body: &Value) -> OpResult {
    let id = as_str(param(body, "ClusterId")).unwrap_or_default();
    match state.clusters.get(&id) {
        Some(c) => Ok(json!({"Cluster": c.to_json(account, region)})),
        None => Err(invalid_cluster(&id)),
    }
}

/// ListClusters — state + created filters, sorted by id (moto list_clusters).
fn list_clusters(state: &mut EmrState, account: u64, region: &str, body: &Value) -> OpResult {
    let states_wanted = str_list(param(body, "ClusterStates"));
    let created_after = as_f64(param(body, "CreatedAfter"));
    let created_before = as_f64(param(body, "CreatedBefore"));
    let mut clusters: Vec<&Cluster> = state
        .clusters
        .values()
        .filter(|c| {
            states_wanted.is_empty()
                || states_wanted
                    .iter()
                    .any(|w| Some(w.as_str()) == c.state.as_deref())
        })
        .filter(|c| {
            created_after
                .map(|after| c.creation_at > after)
                .unwrap_or(true)
                && created_before
                    .map(|before| c.creation_at < before)
                    .unwrap_or(true)
        })
        .collect();
    clusters.sort_by(|a, b| a.id.cmp(&b.id));
    let list = clusters
        .iter()
        .map(|c| c.summary_json(account, region))
        .collect::<Vec<_>>();
    Ok(json!({"Clusters": list}))
}

/// DescribeJobFlows — legacy JobFlowDetail list (60-day window, id-sorted).
fn describe_job_flows(state: &mut EmrState, body: &Value) -> OpResult {
    let job_flow_ids = str_list(param(body, "JobFlowIds"));
    let job_flow_states = str_list(param(body, "JobFlowStates"));
    let created_after = as_f64(param(body, "CreatedAfter"));
    let created_before = as_f64(param(body, "CreatedBefore"));
    let cutoff = now_ts() - 60.0 * 24.0 * 3600.0;
    let mut clusters: Vec<&Cluster> = state
        .clusters
        .values()
        .filter(|c| c.creation_at >= cutoff)
        .filter(|c| job_flow_ids.is_empty() || job_flow_ids.contains(&c.id))
        .filter(|c| {
            job_flow_states.is_empty()
                || job_flow_states
                    .iter()
                    .any(|w| Some(w.as_str()) == c.state.as_deref())
        })
        .filter(|c| {
            created_after
                .map(|after| c.creation_at > after)
                .unwrap_or(true)
                && created_before
                    .map(|before| c.creation_at < before)
                    .unwrap_or(true)
        })
        .collect();
    clusters.sort_by(|a, b| a.id.cmp(&b.id));
    clusters.truncate(512);
    let flows = clusters
        .iter()
        .map(|c| c.job_flow_json(state))
        .collect::<Vec<_>>();
    Ok(json!({"JobFlows": flows}))
}

/// TerminateJobFlows — unknown ids mirror moto's bare dict read (KeyError).
fn terminate_job_flows(state: &mut EmrState, body: &Value) -> OpResult {
    let ids = str_list(param(body, "JobFlowIds"));
    let mut has_protected = false;
    for id in &ids {
        match state.clusters.get_mut(id) {
            None => return Err(key_error(id)),
            Some(c) if c.termination_protected => has_protected = true,
            Some(c) => terminate_cluster(c),
        }
    }
    if has_protected {
        return Err(validation(
            "Could not shut down one or more job flows since they are termination protected.",
        ));
    }
    Ok(json!({}))
}

fn set_termination_protection(state: &mut EmrState, body: &Value) -> OpResult {
    let value = bool_param(param(body, "TerminationProtected"), false);
    for id in str_list(param(body, "JobFlowIds")) {
        match state.clusters.get_mut(&id) {
            None => return Err(key_error(&id)),
            Some(c) => c.termination_protected = value,
        }
    }
    Ok(json!({}))
}

fn set_visible_to_all_users(state: &mut EmrState, body: &Value) -> OpResult {
    let value = bool_param(param(body, "VisibleToAllUsers"), false);
    for id in str_list(param(body, "JobFlowIds")) {
        match state.clusters.get_mut(&id) {
            None => return Err(key_error(&id)),
            Some(c) => c.visible_to_all_users = value,
        }
    }
    Ok(json!({}))
}

/// Insert-or-overwrite tag pairs (moto dict.update semantics).
fn tag_upsert(c: &mut Cluster, tags: &[(String, String)]) {
    for (k, v) in tags {
        if let Some(slot) = c.tags.iter_mut().find(|(existing, _)| existing == k) {
            slot.1 = v.clone();
        } else {
            c.tags.push((k.clone(), v.clone()));
        }
    }
}

/// AddTags — `{d["Key"]: d["Value"] for d in tags}` under XFormed lookups.
fn add_tags(state: &mut EmrState, body: &Value) -> OpResult {
    let id = as_str(param(body, "ResourceId")).unwrap_or_default();
    let cluster = state
        .clusters
        .get_mut(&id)
        .ok_or_else(|| invalid_cluster(&id))?;
    let tags: Vec<(String, String)> = as_vec(param(body, "Tags"))
        .iter()
        .filter_map(|t| Some((as_str(pick(t, "key"))?, as_str(pick(t, "value"))?)))
        .collect();
    tag_upsert(cluster, &tags);
    Ok(json!({}))
}

fn remove_tags(state: &mut EmrState, body: &Value) -> OpResult {
    let id = as_str(param(body, "ResourceId")).unwrap_or_default();
    let keys = str_list(param(body, "TagKeys"));
    let cluster = state
        .clusters
        .get_mut(&id)
        .ok_or_else(|| invalid_cluster(&id))?;
    cluster.tags.retain(|(k, _)| !keys.contains(k));
    Ok(json!({}))
}

/// ModifyCluster — updates StepConcurrencyLevel and echoes it (None → {}).
fn modify_cluster(state: &mut EmrState, body: &Value) -> OpResult {
    let id = as_str(param(body, "ClusterId")).unwrap_or_default();
    let level = as_i64(param(body, "StepConcurrencyLevel"));
    let cluster = state
        .clusters
        .get_mut(&id)
        .ok_or_else(|| invalid_cluster(&id))?;
    cluster.step_concurrency_level = level;
    let mut response = Map::new();
    put_opt_num(&mut response, "StepConcurrencyLevel", level);
    Ok(Value::Object(response))
}

// ---------------------------------------------------------------------------
// Family B handlers
// ---------------------------------------------------------------------------

/// AddJobFlowSteps — mirrors moto add_steps semantics (first step RUNNING,
/// later ones PENDING, cluster flips RUNNING).
fn add_job_flow_steps(state: &mut EmrState, body: &Value) -> OpResult {
    let id = as_str(param(body, "JobFlowId")).unwrap_or_default();
    let cluster = state
        .clusters
        .get_mut(&id)
        .ok_or_else(|| invalid_cluster(&id))?;
    let mut ids = Vec::new();
    for step in as_vec(param(body, "Steps")).iter() {
        let state_name = if cluster.steps.is_empty() {
            "RUNNING"
        } else {
            "PENDING"
        };
        let step = Step {
            id: random_step_id(),
            name: as_str(pick(step, "name")),
            action_on_failure: as_str(pick(step, "action_on_failure")),
            hadoop_jar_step: pick(step, "hadoop_jar_step").cloned().unwrap_or(json!({})),
            created_at: now_ts(),
            seq: next_seq(),
            start_at: None,
            end_at: None,
            state: state_name.into(),
        };
        ids.push(step.id.clone());
        cluster.steps.push(step);
    }
    cluster.state = Some("RUNNING".into());
    Ok(json!({"StepIds": ids}))
}

/// ListSteps — steps sorted newest-first with optional id/state filters.
fn list_steps(state: &mut EmrState, body: &Value) -> OpResult {
    let id = as_str(param(body, "ClusterId")).unwrap_or_default();
    let step_ids = str_list(param(body, "StepIds"));
    let step_states = str_list(param(body, "StepStates"));
    let mut steps: Vec<&Step> = state
        .clusters
        .get(&id)
        .ok_or_else(|| invalid_cluster(&id))?
        .steps
        .iter()
        .filter(|s| step_ids.is_empty() || step_ids.contains(&s.id))
        .filter(|s| step_states.is_empty() || step_states.contains(&s.state))
        .collect();
    steps.sort_by_key(|a| std::cmp::Reverse(a.seq));
    let list = steps
        .iter()
        .map(|s| step_summary_json(s))
        .collect::<Vec<_>>();
    Ok(json!({"Steps": list}))
}

/// DescribeStep — unknown step inside a valid cluster yields `{}` (moto None).
fn describe_step(state: &mut EmrState, body: &Value) -> OpResult {
    let id = as_str(param(body, "ClusterId")).unwrap_or_default();
    let step_id = as_str(param(body, "StepId")).unwrap_or_default();
    let cluster = state
        .clusters
        .get(&id)
        .ok_or_else(|| invalid_cluster(&id))?;
    match cluster.steps.iter().find(|s| s.id == step_id) {
        Some(s) => Ok(json!({"Step": step_summary_json(s)})),
        None => Ok(json!({})),
    }
}

/// CancelSteps — per-id status records like moto cancel_steps.
fn cancel_steps(state: &mut EmrState, body: &Value) -> OpResult {
    let id = as_str(param(body, "ClusterId")).unwrap_or_default();
    let step_ids = str_list(param(body, "StepIds"));
    let cluster = state
        .clusters
        .get_mut(&id)
        .ok_or_else(|| invalid_cluster(&id))?;
    let mut out = Vec::new();
    for step_id in step_ids {
        let step = cluster.steps.iter_mut().find(|s| s.id == step_id);
        match step {
            None => out.push(json!({
                "StepId": step_id,
                "Status": "FAILED",
                "Reason": format!("Step {step_id} is not found."),
            })),
            Some(s) if matches!(s.state.as_str(), "COMPLETED" | "FAILED" | "CANCELLED") => {
                out.push(json!({
                    "StepId": step_id,
                    "Status": "FAILED",
                    "Reason": format!("Step {step_id} could not be cancelled."),
                }));
            }
            Some(s) => {
                s.state = "CANCELLED".into();
                s.end_at = Some(now_ts());
                out.push(json!({"StepId": step_id, "Status": "SUCCESS", "Reason": "NONE"}));
            }
        }
    }
    Ok(json!({"CancelStepsInfoList": out}))
}

/// AddInstanceGroups — response renders `{}`: moto's handler returns group
/// objects under a key the (patched-spec) output shape does not declare, so
/// the value picker drops everything.
fn add_instance_groups(state: &mut EmrState, body: &Value) -> OpResult {
    let job_flow_id = as_str(param(body, "JobFlowId")).unwrap_or_default();
    if !state.clusters.contains_key(&job_flow_id) {
        return Err(invalid_cluster(&job_flow_id));
    }
    for group in as_vec(param(body, "InstanceGroups")).iter() {
        let role = match as_str(pick(group, "instance_role")) {
            Some(r) if matches!(r.as_str(), "MASTER" | "CORE" | "TASK") => r,
            _ => String::new(), // moto would treat kwargs loosely
        };
        let mut built = InstanceGroup {
            id: random_instance_group_id(),
            name: as_str(pick(group, "name")),
            role: role.clone(),
            instance_type: as_str(pick(group, "instance_type")),
            num_instances: as_i64(pick(group, "instance_count")).unwrap_or(0),
            market: as_str(pick(group, "market")).unwrap_or_else(|| "ON_DEMAND".into()),
            bid_price: as_str(pick(group, "bid_price")),
            auto_scaling_policy: None,
            ebs_configuration: pick(group, "ebs_configuration").cloned(),
            created_at: now_ts(),
            ready_at: now_ts(),
        };
        if built.name.is_none() {
            built.name = Some(match role.as_str() {
                "MASTER" => "master".into(),
                "CORE" => "slave".into(),
                _ => "Task instance group".into(),
            });
        }
        link_group(state, &job_flow_id, built);
    }
    Ok(json!({}))
}

/// moto Cluster.add_instance_group: MASTER/CORE bookkeeping + id append.
fn link_group(state: &mut EmrState, cluster_id: &str, group: InstanceGroup) {
    let role = group.role.clone();
    let num_instances = group.num_instances;
    let group_id = group.id.clone();
    state.instance_groups.insert(group_id.clone(), group);
    if let Some(c) = state.clusters.get_mut(cluster_id) {
        if role == "MASTER" {
            c.master_instance_group_id = Some(group_id.clone());
            if num_instances > 1 {
                // moto: HA cluster (exactly 3) pins keep-alive + protection
                c.keep_job_flow_alive_when_no_steps = true;
                c.termination_protected = num_instances == 3;
            }
        }
        if role == "CORE" {
            c.core_instance_group_id = Some(group_id.clone());
        }
        c.instance_group_ids.push(group_id);
    }
}

/// ListInstanceGroups — groups sorted by id (moto list_instance_groups).
fn list_instance_groups(state: &mut EmrState, body: &Value) -> OpResult {
    let id = as_str(param(body, "ClusterId")).unwrap_or_default();
    let cluster = state
        .clusters
        .get(&id)
        .ok_or_else(|| invalid_cluster(&id))?;
    Ok(json!({
        "InstanceGroups": list_instance_groups_json(state, cluster),
    }))
}

/// ModifyInstanceGroups — bare dict reads; missing group/count as moto.
fn modify_instance_groups(state: &mut EmrState, body: &Value) -> OpResult {
    for group in as_vec(param(body, "InstanceGroups")).iter() {
        let group_id = as_str(pick(group, "instance_group_id"));
        let count = as_i64(pick(group, "instance_count"));
        let found = group_id
            .as_ref()
            .and_then(|gid| state.instance_groups.get_mut(gid));
        match found {
            None => {
                return Err(key_error(
                    group_id
                        .as_deref()
                        .or(Some("instance_count"))
                        .unwrap_or("instance_count"),
                ));
            }
            Some(g) => match count {
                None => return Err(key_error("instance_count")),
                Some(n) => g.num_instances = n,
            },
        }
    }
    Ok(json!({}))
}

/// ListInstances — no EC2-instance bookkeeping (moto only wires instances
/// when Instances.InstanceGroups accompanies RunJobFlow; the suites don't).
fn list_instances(state: &mut EmrState, body: &Value) -> OpResult {
    let id = as_str(param(body, "ClusterId")).unwrap_or_default();
    state
        .clusters
        .get(&id)
        .ok_or_else(|| invalid_cluster(&id))?;
    let _ = param(body, "InstanceGroupId");
    let _ = param(body, "InstanceGroupTypes");
    Ok(json!({"Instances": []}))
}

/// ListBootstrapActions — Command shape {Name, ScriptPath, Args}.
fn list_bootstrap_actions(state: &mut EmrState, body: &Value) -> OpResult {
    let id = as_str(param(body, "ClusterId")).unwrap_or_default();
    let cluster = state
        .clusters
        .get(&id)
        .ok_or_else(|| invalid_cluster(&id))?;
    let actions = cluster
        .bootstrap_actions
        .iter()
        .map(|a| {
            json!({
                "Name": a.name,
                "ScriptPath": a.script_path,
                "Args": a.args,
            })
        })
        .collect::<Vec<_>>();
    Ok(json!({"BootstrapActions": actions}))
}

/// AddInstanceFleet — stores the fleet, echoes cluster/id/arn.
fn add_instance_fleet(state: &mut EmrState, account: u64, region: &str, body: &Value) -> OpResult {
    let cluster_id = as_str(param(body, "ClusterId")).unwrap_or_default();
    if !state.clusters.contains_key(&cluster_id) {
        return Err(invalid_cluster(&cluster_id));
    }
    let fleet_req = param(body, "InstanceFleet").or_else(|| param(body, "instance_fleet"));
    // InstanceFleetType default TASK; capacities default 0 (moto defaults).
    let id_fleet = random_instance_fleet_id();
    let fleet = Fleet {
        id: id_fleet.clone(),
        name: as_str(fleet_req.and_then(|f| pick(f, "name"))).unwrap_or_default(),
        instance_fleet_type: as_str(fleet_req.and_then(|f| pick(f, "instance_fleet_type")))
            .unwrap_or_else(|| "TASK".into()),
        target_on_demand_capacity: as_i64(
            fleet_req.and_then(|f| pick(f, "target_on_demand_capacity")),
        )
        .unwrap_or(0),
        target_spot_capacity: as_i64(fleet_req.and_then(|f| pick(f, "target_spot_capacity")))
            .unwrap_or(0),
        provisioned_on_demand_capacity: as_i64(
            fleet_req.and_then(|f| pick(f, "target_on_demand_capacity")),
        )
        .unwrap_or(0),
        provisioned_spot_capacity: as_i64(fleet_req.and_then(|f| pick(f, "target_spot_capacity")))
            .unwrap_or(0),
        instance_type_configs: as_vec(fleet_req.and_then(|f| pick(f, "instance_type_configs"))),
        launch_specifications: fleet_req
            .and_then(|f| pick(f, "launch_specifications"))
            .cloned()
            .unwrap_or(json!({})),
        resize_specifications: fleet_req
            .and_then(|f| pick(f, "resize_specifications"))
            .cloned(),
        cluster_id: cluster_id.clone(),
    };
    let arn = cluster_arn(account, region, &cluster_id);
    state.instance_fleets.push(fleet);
    Ok(json!({
        "ClusterId": cluster_id,
        "InstanceFleetId": id_fleet,
        "ClusterArn": arn,
    }))
}

/// ModifyInstanceFleet — capacity updates; unknown fleet → ResourceNotFound.
fn modify_instance_fleet(state: &mut EmrState, body: &Value) -> OpResult {
    let cluster_id = as_str(param(body, "ClusterId")).unwrap_or_default();
    if !state.clusters.contains_key(&cluster_id) {
        return Err(invalid_cluster(&cluster_id));
    }
    let fleet_req = param(body, "InstanceFleet").or_else(|| param(body, "instance_fleet"));
    let fleet_id = fleet_req
        .and_then(|f| as_str(pick(f, "instance_fleet_id")))
        .unwrap_or_default();
    let fleet = state
        .instance_fleets
        .iter_mut()
        .find(|f| f.id == fleet_id)
        .ok_or_else(|| {
            resource_not_found(&format!("Instance fleet '{}' is not found.", fleet_id))
        })?;
    if let Some(v) = fleet_req.and_then(|f| as_i64(pick(f, "target_on_demand_capacity"))) {
        fleet.target_on_demand_capacity = v;
        fleet.provisioned_on_demand_capacity = v;
    }
    if let Some(v) = fleet_req.and_then(|f| as_i64(pick(f, "target_spot_capacity"))) {
        fleet.target_spot_capacity = v;
        fleet.provisioned_spot_capacity = v;
    }
    if let Some(rs) = fleet_req
        .and_then(|f| pick(f, "resize_specifications"))
        .cloned()
    {
        fleet.resize_specifications = Some(rs);
    }
    Ok(json!({}))
}

/// ListInstanceFleets — insertion order, mirroring moto's dict walk.
fn list_instance_fleets(
    state: &mut EmrState,
    account: u64,
    region: &str,
    body: &Value,
) -> OpResult {
    let cluster_id = as_str(param(body, "ClusterId")).unwrap_or_default();
    if !state.clusters.contains_key(&cluster_id) {
        return Err(invalid_cluster(&cluster_id));
    }
    let fleets = state
        .instance_fleets
        .iter()
        .filter(|f| f.cluster_id == cluster_id)
        .map(|f| fleet_json(f, account, region))
        .collect::<Vec<_>>();
    Ok(json!({"InstanceFleets": fleets}))
}

/// InstanceFleet element (moto responses.list_instance_fleets).
fn fleet_json(f: &Fleet, _account: u64, _region: &str) -> Value {
    json!({
        "Id": f.id,
        "Name": f.name,
        "Status": {
            "State": "RUNNING",
            "StateChangeReason": {"Code": "USER_REQUEST", "Message": ""},
            "Timeline": {"CreationDateTime": now_ts(), "ReadyDateTime": now_ts()},
        },
        "InstanceFleetType": f.instance_fleet_type,
        "TargetOnDemandCapacity": f.target_on_demand_capacity,
        "TargetSpotCapacity": f.target_spot_capacity,
        "ProvisionedOnDemandCapacity": f.provisioned_on_demand_capacity,
        "ProvisionedSpotCapacity": f.provisioned_spot_capacity,
        "InstanceTypeSpecifications": f
            .instance_type_configs
            .iter()
            .map(|c| {
                let mut obj = Map::new();
                if let Some(t) = as_str(pick(c, "instance_type")) {
                    obj.insert("InstanceType".into(), json!(t));
                }
                if let Some(v) = as_i64(pick(c, "weighted_capacity")) {
                    obj.insert("WeightedCapacity".into(), json!(v));
                }
                Value::Object(obj)
            })
            .collect::<Vec<_>>(),
        "LaunchSpecifications": f.launch_specifications,
    })
}

/// PutAutoScalingPolicy — attaches (snake-stored, status-injected) policy.
fn put_auto_scaling_policy(
    state: &mut EmrState,
    account: u64,
    region: &str,
    body: &Value,
) -> OpResult {
    let cluster_id = as_str(param(body, "ClusterId")).unwrap_or_default();
    let cluster = state
        .clusters
        .get(&cluster_id)
        .ok_or_else(|| invalid_cluster(&cluster_id))?;
    let cluster_arn_v = cluster.arn(account, region);
    // moto: `assert instance_group is not None` → InternalError("") when absent.
    let group_id = as_str(param(body, "InstanceGroupId")).unwrap_or_default();
    let group = state.instance_groups.get_mut(&group_id).ok_or_else(|| {
        // moto: `assert instance_group is not None` → the werkzeug HTML 500.
        crate::proto::moto_crash_response()
    })?;
    group.auto_scaling_policy =
        attach_auto_scaling_policy(param(body, "AutoScalingPolicy"), &cluster_id.clone());
    let policy = group
        .auto_scaling_policy
        .as_ref()
        .map(policy_snake_to_pascal);
    let gid = group.id.clone();
    let mut response = Map::new();
    response.insert("ClusterId".into(), json!(cluster_id));
    response.insert("InstanceGroupId".into(), json!(gid));
    if let Some(policy) = policy {
        response.insert("AutoScalingPolicy".into(), policy);
    }
    response.insert("ClusterArn".into(), json!(cluster_arn_v));
    Ok(Value::Object(response))
}

/// RemoveAutoScalingPolicy — moto routes through put(policy=None).
fn remove_auto_scaling_policy(state: &mut EmrState, body: &Value) -> OpResult {
    let group_id = as_str(param(body, "InstanceGroupId")).unwrap_or_default();
    if let Some(group) = state.instance_groups.get_mut(&group_id) {
        group.auto_scaling_policy = None;
    }
    Ok(json!({}))
}

// ---------------------------------------------------------------------------
// Family C handlers
// ---------------------------------------------------------------------------

/// CreateSecurityConfiguration — duplicate name → generic InvalidRequest
/// (moto's keyword-message form loses the text).
fn create_security_configuration(state: &mut EmrState, body: &Value) -> OpResult {
    let name = as_str(param(body, "Name")).unwrap_or_default();
    if state.security_configurations.contains_key(&name) {
        return Err(generic_invalid_request());
    }
    let created_at = now_ts();
    state.security_configurations.insert(
        name.clone(),
        SecurityConfiguration {
            name: name.clone(),
            body: as_str(param(body, "SecurityConfiguration")).unwrap_or_default(),
            created_at,
        },
    );
    Ok(json!({"Name": name, "CreationDateTime": created_at}))
}

/// DescribeSecurityConfiguration — missing → ResourceNotFound.
fn describe_security_configuration(state: &mut EmrState, body: &Value) -> OpResult {
    let name = as_str(param(body, "Name")).unwrap_or_default();
    match state.security_configurations.get(&name) {
        Some(c) => Ok(json!({
            "Name": c.name,
            "SecurityConfiguration": c.body,
            "CreationDateTime": c.created_at,
        })),
        None => Err(resource_not_found(&format!(
            "Security configuration with name '{}' does not exist.",
            name
        ))),
    }
}

fn delete_security_configuration(state: &mut EmrState, body: &Value) -> OpResult {
    let name = as_str(param(body, "Name")).unwrap_or_default();
    if state.security_configurations.remove(&name).is_none() {
        return Err(resource_not_found(&format!(
            "Security configuration with name '{}' does not exist.",
            name
        )));
    }
    Ok(json!({}))
}

fn list_security_configurations(state: &mut EmrState, _body: &Value) -> OpResult {
    let list = state
        .security_configurations
        .values()
        .map(|c| json!({"Name": c.name, "CreationDateTime": c.created_at}))
        .collect::<Vec<_>>();
    Ok(json!({"SecurityConfigurations": list}))
}

/// PutBlockPublicAccessConfiguration — moto stores the
/// block_public_access_configuration(+metadata) snake split.
fn put_block_public_access_configuration(
    state: &mut EmrState,
    account: u64,
    body: &Value,
) -> OpResult {
    let config = param(body, "BlockPublicAccessConfiguration");
    // moto: `x.get("BlockPublicSecurityGroupRules") or True` — falsy → True.
    let rules = match config.and_then(|c| pick(c, "block_public_security_group_rules")) {
        Some(Value::Bool(false)) | None => json!(true),
        Some(other) if !other.is_null() => other.clone(),
        _ => json!(true),
    };
    let ranges =
        as_vec(config.and_then(|c| pick(c, "permitted_public_security_group_rule_ranges")))
            .into_iter()
            .map(|r| {
                json!({
                    "min_range": pick(&r, "min_range").cloned().unwrap_or(Value::Null),
                    "max_range": pick(&r, "max_range").cloned().unwrap_or(Value::Null),
                })
            })
            .collect::<Vec<_>>();
    state.block_public_access = Some(json!({
        "block_public_access_configuration": {
            "block_public_security_group_rules": rules,
            "permitted_public_security_group_rule_ranges": ranges,
        },
        "block_public_access_configuration_metadata": {
            "creation_date_time": now_ts(),
            "created_by_arn": format!("arn:aws:sts::{}:user/moto", account),
        },
    }));
    Ok(json!({}))
}

/// GetBlockPublicAccessConfiguration — never errors; unset store yields
/// empty members (the lifecycle suite pins this "no exception" behavior).
fn get_block_public_access_configuration(state: &mut EmrState, _body: &Value) -> OpResult {
    let stored = state.block_public_access.clone().unwrap_or(json!({}));
    let mut rendered = Map::new();
    let mut config = Map::new();
    if let Some(rules) = param(
        &stored,
        "block_public_access_configuration.block_public_security_group_rules",
    ) {
        config.insert("BlockPublicSecurityGroupRules".into(), rules.clone());
    }
    if let Some(ranges) = param(
        &stored,
        "block_public_access_configuration.permitted_public_security_group_rule_ranges",
    ) {
        let list = as_vec(Some(ranges))
            .iter()
            .map(|r| {
                let mut entry = Map::new();
                put_option_value(&mut entry, "MinRange", pick(r, "min_range"));
                put_option_value(&mut entry, "MaxRange", pick(r, "max_range"));
                Value::Object(entry)
            })
            .collect::<Vec<_>>();
        config.insert("PermittedPublicSecurityGroupRuleRanges".into(), json!(list));
    }
    let mut meta = Map::new();
    if let Some(created) = as_f64(param(
        &stored,
        "block_public_access_configuration_metadata.creation_date_time",
    )) {
        meta.insert("CreationDateTime".into(), json!(created));
    }
    if let Some(arn) = as_str(param(
        &stored,
        "block_public_access_configuration_metadata.created_by_arn",
    )) {
        meta.insert("CreatedByArn".into(), json!(arn));
    }
    rendered.insert(
        "BlockPublicAccessConfiguration".into(),
        Value::Object(config),
    );
    rendered.insert(
        "BlockPublicAccessConfigurationMetadata".into(),
        Value::Object(meta),
    );
    Ok(Value::Object(rendered))
}

fn put_option_value(m: &mut Map<String, Value>, key: &str, v: Option<&Value>) {
    if let Some(v) = v {
        m.insert(key.to_string(), v.clone());
    }
}

/// CreateStudio — {"StudioId", "Url"}.
fn create_studio(state: &mut EmrState, region: &str, body: &Value) -> OpResult {
    let id = random_studio_id();
    let studio = Studio {
        id: id.clone(),
        name: as_str(param(body, "Name")),
        auth_mode: as_str(param(body, "AuthMode")),
        vpc_id: as_str(param(body, "VpcId")),
        subnet_ids: as_vec(param(body, "SubnetIds")),
        service_role: as_str(param(body, "ServiceRole")),
        user_role: as_str(param(body, "UserRole")).unwrap_or_default(),
        workspace_security_group_id: as_str(param(body, "WorkspaceSecurityGroupId")),
        engine_security_group_id: as_str(param(body, "EngineSecurityGroupId")),
        default_s3_location: as_str(param(body, "DefaultS3Location")).unwrap_or_default(),
        description: as_str(param(body, "Description")).unwrap_or_default(),
        created_at: now_ts(),
        tags: as_vec(param(body, "Tags"))
            .iter()
            .filter_map(|t| Some((as_str(pick(t, "key"))?, as_str(pick(t, "value"))?)))
            .collect(),
    };
    let url = format!("https://{}.emrstudio-prod.{}.amazonaws.com", id, region);
    state.studios.insert(id.clone(), studio);
    Ok(json!({"StudioId": id, "Url": url}))
}

/// DescribeStudio — {"Studio": …} or generic InvalidRequestException.
fn describe_studio(state: &mut EmrState, account: u64, region: &str, body: &Value) -> OpResult {
    let id = as_str(param(body, "StudioId")).unwrap_or_default();
    let studio = state.studios.get(&id).ok_or_else(generic_invalid_request)?;
    Ok(json!({"Studio": studio_json(studio, account, region)}))
}

fn studio_json(s: &Studio, account: u64, region: &str) -> Value {
    let mut obj = Map::new();
    obj.insert("StudioId".into(), json!(s.id));
    obj.insert(
        "StudioArn".into(),
        json!(format!(
            "arn:aws:elasticmapreduce:{region}:{account}:studio/{}",
            s.id
        )),
    );
    put_opt_str(&mut obj, "Name", s.name.as_ref());
    obj.insert("Description".into(), json!(s.description));
    put_opt_str(&mut obj, "AuthMode", s.auth_mode.as_ref());
    put_opt_str(&mut obj, "VpcId", s.vpc_id.as_ref());
    obj.insert("SubnetIds".into(), json!(s.subnet_ids));
    put_opt_str(&mut obj, "ServiceRole", s.service_role.as_ref());
    obj.insert("UserRole".into(), json!(s.user_role));
    put_opt_str(
        &mut obj,
        "WorkspaceSecurityGroupId",
        s.workspace_security_group_id.as_ref(),
    );
    put_opt_str(
        &mut obj,
        "EngineSecurityGroupId",
        s.engine_security_group_id.as_ref(),
    );
    obj.insert(
        "Url".into(),
        json!(format!(
            "https://{}.emrstudio-prod.{}.amazonaws.com",
            s.id, region
        )),
    );
    obj.insert("CreationTime".into(), json!(s.created_at));
    obj.insert("DefaultS3Location".into(), json!(s.default_s3_location));
    obj.insert(
        "Tags".into(),
        json!(s
            .tags
            .iter()
            .map(|(k, v)| json!({"Key": k, "Value": v}))
            .collect::<Vec<_>>()),
    );
    Value::Object(obj)
}

fn delete_studio(state: &mut EmrState, body: &Value) -> OpResult {
    let id = as_str(param(body, "StudioId")).unwrap_or_default();
    if state.studios.remove(&id).is_none() {
        return Err(generic_invalid_request());
    }
    Ok(json!({}))
}

/// ListStudios — StudioSummary members (moto list_studios).
fn list_studios(state: &mut EmrState, region: &str, _body: &Value) -> OpResult {
    let studios = state
        .studios
        .values()
        .map(|s| {
            json!({
                "StudioId": s.id,
                "Name": s.name,
                "Description": s.description,
                "Url": format!("https://{}.emrstudio-prod.{}.amazonaws.com", s.id, region),
                "AuthMode": s.auth_mode,
                "CreationTime": s.created_at,
            })
        })
        .collect::<Vec<_>>();
    Ok(json!({"Studios": studios}))
}

/// CreateStudioSessionMapping — requires an existing studio; the stored
/// identity_id defaults to "" (moto `self._get_param("IdentityId", "")`).
fn create_studio_session_mapping(state: &mut EmrState, body: &Value) -> OpResult {
    let studio_id = as_str(param(body, "StudioId")).unwrap_or_default();
    if !state.studios.contains_key(&studio_id) {
        return Err(generic_invalid_request());
    }
    let identity_id = as_str(param(body, "IdentityId")).unwrap_or_default();
    let now = now_ts();
    let mapping = StudioSessionMapping {
        studio_id,
        identity_id,
        identity_name: as_str(param(body, "IdentityName")),
        identity_type: as_str(param(body, "IdentityType")).unwrap_or_default(),
        session_policy_arn: as_str(param(body, "SessionPolicyArn")),
        created_at: now,
        last_modified_at: now,
    };
    let key = mapping_key(&mapping);
    state.studio_session_mappings.insert(key, mapping);
    Ok(json!({}))
}

/// Store-side key `{studio}:{identity_type}:{identity_id}` (moto's f-string).
fn mapping_key(m: &StudioSessionMapping) -> String {
    format!("{}:{}:{}", m.studio_id, m.identity_type, m.identity_id)
}

/// Lookup key interpolates an absent IdentityId as "None" (moto f-string; the
/// suites lean on that quirk).
fn mapping_lookup_key(studio_id: &str, identity_type: &str, identity_id: Option<&str>) -> String {
    format!(
        "{}:{}:{}",
        studio_id,
        identity_type,
        identity_id.unwrap_or("None")
    )
}

fn get_studio_session_mapping(state: &mut EmrState, body: &Value) -> OpResult {
    let key = mapping_lookup_key(
        &as_str(param(body, "StudioId")).unwrap_or_default(),
        &as_str(param(body, "IdentityType")).unwrap_or_default(),
        as_str(param(body, "IdentityId")).as_deref(),
    );
    match state.studio_session_mappings.get(&key) {
        Some(m) => Ok(json!({
            "SessionMapping": {
                "StudioId": m.studio_id,
                "IdentityId": m.identity_id,
                "IdentityName": m.identity_name,
                "IdentityType": m.identity_type,
                "SessionPolicyArn": m.session_policy_arn,
                "CreationTime": m.created_at,
                "LastModifiedTime": m.last_modified_at,
            }
        })),
        None => Err(generic_invalid_request()),
    }
}

fn update_studio_session_mapping(state: &mut EmrState, body: &Value) -> OpResult {
    let key = mapping_lookup_key(
        &as_str(param(body, "StudioId")).unwrap_or_default(),
        &as_str(param(body, "IdentityType")).unwrap_or_default(),
        as_str(param(body, "IdentityId")).as_deref(),
    );
    match state.studio_session_mappings.get_mut(&key) {
        Some(m) => {
            m.session_policy_arn = as_str(param(body, "SessionPolicyArn"));
            m.last_modified_at = now_ts();
            Ok(json!({}))
        }
        None => Err(generic_invalid_request()),
    }
}

fn delete_studio_session_mapping(state: &mut EmrState, body: &Value) -> OpResult {
    let key = mapping_lookup_key(
        &as_str(param(body, "StudioId")).unwrap_or_default(),
        &as_str(param(body, "IdentityType")).unwrap_or_default(),
        as_str(param(body, "IdentityId")).as_deref(),
    );
    if state.studio_session_mappings.remove(&key).is_none() {
        return Err(generic_invalid_request());
    }
    Ok(json!({}))
}

/// DescribeNotebookExecution — generic InvalidRequestException when missing.
fn describe_notebook_execution(state: &mut EmrState, body: &Value) -> OpResult {
    let id = as_str(param(body, "NotebookExecutionId")).unwrap_or_default();
    match state.notebook_executions.get(&id) {
        Some(e) => Ok(json!({
            "NotebookExecution": {
                "NotebookExecutionId": e.id,
                "NotebookExecutionName": e.name,
                "EditorId": e.editor_id,
                "ExecutionEngine": e.execution_engine,
                "ServiceRole": e.service_role,
                "NotebookParams": e.params,
                "Status": e.status,
                "StartTime": e.start_at,
                "EndTime": e.end_at,
                "Tags": [],
            }
        })),
        None => Err(generic_invalid_request()),
    }
}

fn stop_notebook_execution(state: &mut EmrState, body: &Value) -> OpResult {
    let id = as_str(param(body, "NotebookExecutionId")).unwrap_or_default();
    match state.notebook_executions.get_mut(&id) {
        Some(e) => {
            e.status = "STOPPED".into();
            e.end_at = Some(now_ts());
            Ok(json!({}))
        }
        None => Err(generic_invalid_request()),
    }
}

fn list_notebook_executions(state: &mut EmrState, body: &Value) -> OpResult {
    let status_filter = as_str(param(body, "Status"));
    let list = state
        .notebook_executions
        .values()
        .filter(|e| status_filter.as_ref().is_none_or(|st| st == &e.status))
        .map(|e| {
            json!({
                "NotebookExecutionId": e.id,
                "NotebookExecutionName": e.name,
                "EditorId": e.editor_id,
                "Status": e.status,
                "StartTime": e.start_at,
                "EndTime": e.end_at,
            })
        })
        .collect::<Vec<_>>();
    Ok(json!({"NotebookExecutions": list}))
}

/// StartNotebookExecution — moto's `create_notebook_execution` backend
/// (moto routes it under the patched-spec action name; no cluster-id
/// validation, the suites start executions against fake clusters).
fn start_notebook_execution(
    state: &mut EmrState,
    account: u64,
    region: &str,
    body: &Value,
) -> OpResult {
    let id = format!("ex-{}", random_id());
    let execution = NotebookExecution {
        id: id.clone(),
        editor_id: as_str(param(body, "EditorId")).unwrap_or_default(),
        execution_engine: param(body, "ExecutionEngine").cloned().unwrap_or(json!({})),
        service_role: as_str(param(body, "ServiceRole")).unwrap_or_default(),
        name: as_str(param(body, "NotebookExecutionName")).unwrap_or_default(),
        params: as_str(param(body, "NotebookParams")).unwrap_or_default(),
        status: "STARTING".into(),
        start_at: now_ts(),
        end_at: None,
    };
    state.notebook_executions.insert(id.clone(), execution);
    let _ = (account, region);
    Ok(json!({"NotebookExecutionId": id}))
}

/// SetKeepJobFlowAliveWhenNoSteps — flips the flag on clusters it finds;
/// unknown job-flow ids still succeed (suite pins 200 for fake ids).
fn set_keep_job_flow_alive_when_no_steps(state: &mut EmrState, body: &Value) -> OpResult {
    let keep = bool_param(param(body, "KeepJobFlowAliveWhenNoSteps"), false);
    for id in str_list(param(body, "JobFlowIds")) {
        if let Some(cluster) = state.clusters.get_mut(&id) {
            cluster.keep_job_flow_alive_when_no_steps = keep;
        }
    }
    Ok(json!({}))
}

/// SetUnhealthyNodeReplacement — acknowledged without state (the flag has no
/// rendered surface; suites pin 200 even for unknown job-flow ids).
fn set_unhealthy_node_replacement(body: &Value) -> OpResult {
    let _ = str_list(param(body, "JobFlowIds"));
    let _ = bool_param(param(body, "UnhealthyNodeReplacement"), false);
    Ok(json!({}))
}

/// GetClusterSessionCredentials — synthetic session credentials; no cluster
/// validation (suite pins Credentials + ExpiresAt presence only).
/// `Credentials` is a botocore union: exactly one member (UsernamePassword).
fn get_cluster_session_credentials(body: &Value) -> OpResult {
    let _ = as_str(param(body, "ClusterId"));
    let secret: String = uuid::Uuid::new_v4()
        .as_bytes()
        .iter()
        .flat_map(|b| [format!("{:02x}", b), format!("{:02x}", b)])
        .take(40)
        .collect();
    Ok(json!({
        "Credentials": {
            "UsernamePassword": {
                "Username": format!("hadoop-{}", random_id().to_lowercase()),
                "Password": secret,
            }
        },
        "ExpiresAt": now_ts() + 12.0 * 3600.0,
    }))
}

/// ListStudioSessionMappings — moto's model keeps the mappings but its
/// responses table lacks the route; render the stored mappings with the
/// same member set as GetStudioSessionMapping, optionally filtered.
fn list_studio_session_mappings(state: &mut EmrState, body: &Value) -> OpResult {
    let studio_filter = as_str(param(body, "StudioId"));
    let identity_type_filter = as_str(param(body, "IdentityType"));
    let mut items: Vec<&StudioSessionMapping> = state
        .studio_session_mappings
        .values()
        .filter(|m| studio_filter.as_ref().is_none_or(|s| s == &m.studio_id))
        .filter(|m| {
            identity_type_filter
                .as_ref()
                .is_none_or(|t| t == &m.identity_type)
        })
        .collect();
    items.sort_by(|a, b| {
        a.created_at
            .partial_cmp(&b.created_at)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let session_mappings = items
        .into_iter()
        .map(|m| {
            json!({
                "StudioId": m.studio_id,
                "IdentityId": m.identity_id,
                "IdentityName": m.identity_name,
                "IdentityType": m.identity_type,
                "SessionPolicyArn": m.session_policy_arn,
                "CreationTime": m.created_at,
                "LastModifiedTime": m.last_modified_at,
            })
        })
        .collect::<Vec<_>>();
    Ok(json!({"SessionMappings": session_mappings}))
}

/// CreatePersistentAppUI — patched-spec output only surfaces the id member.
fn create_persistent_app_ui(
    state: &mut EmrState,
    account: u64,
    region: &str,
    body: &Value,
) -> OpResult {
    let id = uuid::Uuid::new_v4().to_string();
    let now = now_ts();
    state.persistent_app_uis.insert(
        id.clone(),
        PersistentAppUi {
            target_resource_arn: as_str(param(body, "TargetResourceArn")).unwrap_or_default(),
            id: id.clone(),
            created_at: now,
            last_modified_at: now,
        },
    );
    let _ = (account, region);
    Ok(json!({"PersistentAppUIId": id}))
}

fn describe_persistent_app_ui(
    state: &mut EmrState,
    account: u64,
    region: &str,
    body: &Value,
) -> OpResult {
    let id = as_str(param(body, "PersistentAppUIId")).unwrap_or_default();
    let ui = state
        .persistent_app_uis
        .get(&id)
        .ok_or_else(|| resource_not_found(&format!("PersistentAppUI {id} does not exist.")))?;
    Ok(json!({
        "PersistentAppUI": {
            "PersistentAppUIId": ui.id,
            "PersistentAppUIStatus": "ACTIVE",
            "TargetResourceArn": ui.target_resource_arn,
            "CreationTime": ui.created_at,
            "LastModifiedTime": ui.last_modified_at,
            "PersistentAppUIArn": format!("arn:aws:elasticmapreduce:{}:{}:persistent-app-ui/{}", region, account, ui.id),
        }
    }))
}

/// GetPersistentAppUIPresignedURL — requires an existing PersistentAppUI
/// (same lookup as DescribePersistentAppUI); the URL itself is synthetic.
fn get_persistent_app_ui_presigned_url(
    state: &mut EmrState,
    region: &str,
    body: &Value,
) -> OpResult {
    let id = as_str(param(body, "PersistentAppUIId")).unwrap_or_default();
    let ui = state
        .persistent_app_uis
        .get(&id)
        .ok_or_else(|| resource_not_found(&format!("PersistentAppUI {id} does not exist.")))?;
    Ok(json!({
        "PresignedURL": format!(
            "https://persistence.prod.{region}.amazonaws.com/persistent-app-ui/{}?arn={}",
            ui.id, ui.target_resource_arn
        ),
    }))
}

/// GetOnClusterAppUIPresignedURL — no cluster validation (suite pins 200
/// against a fake cluster id).
fn get_on_cluster_app_ui_presigned_url(region: &str, body: &Value) -> OpResult {
    let cluster_id = as_str(param(body, "ClusterId")).unwrap_or_default();
    let ui_type = as_str(param(body, "OnClusterAppUIType")).unwrap_or_default();
    let _ = as_str(param(body, "ExecutionRoleArn"));
    Ok(json!({
        "PresignedURL": format!(
            "https://on-cluster-uis.prod.{region}.amazonaws.com/on-cluster-ui/{ui_type}/{cluster_id}"
        ),
    }))
}

/// DescribeReleaseLabel — labels come from moto's us-east-1 resource list.
fn describe_release_label(state: &mut EmrState, body: &Value) -> OpResult {
    let label = as_str(param(body, "ReleaseLabel")).unwrap_or_default();
    if !release_labels().contains(&label) {
        return Err(generic_invalid_request());
    }
    let _ = state;
    Ok(json!({
        "ReleaseLabel": label,
        "Applications": [],
        "AvailableOSReleases": [],
    }))
}

/// ListReleaseLabels — moto's static per-region list (us-east-1 set).
fn list_release_labels() -> OpResult {
    Ok(json!({"ReleaseLabels": release_labels()}))
}

fn release_labels() -> Vec<String> {
    serde_json::from_str(RELEASE_LABELS_US_EAST_1).unwrap_or_default()
}

/// ListSupportedInstanceTypes — moto raises ValidationException for labels
/// without an instance-types resource; emr-6.10.0 ships embedded.
fn list_supported_instance_types(body: &Value) -> OpResult {
    let label = as_str(param(body, "ReleaseLabel")).unwrap_or_default();
    let list: Value = if label == "emr-6.10.0" {
        serde_json::from_str(INSTANCE_TYPES_EMR_6_10_0).unwrap_or(json!([]))
    } else {
        return Err(validation(&format!(
            "Release label {label} is not supported."
        )));
    };
    Ok(json!({"SupportedInstanceTypes": list}))
}

// --- managed scaling / auto-termination policies ----------------------------

fn put_managed_scaling_policy(state: &mut EmrState, body: &Value) -> OpResult {
    let cluster_id = as_str(param(body, "ClusterId")).unwrap_or_default();
    if !state.clusters.contains_key(&cluster_id) {
        return Err(invalid_cluster(&cluster_id));
    }
    state.managed_scaling_policies.insert(
        cluster_id,
        param(body, "ManagedScalingPolicy")
            .cloned()
            .unwrap_or(json!({})),
    );
    Ok(json!({}))
}

fn get_managed_scaling_policy(state: &mut EmrState, body: &Value) -> OpResult {
    let cluster_id = as_str(param(body, "ClusterId")).unwrap_or_default();
    if !state.clusters.contains_key(&cluster_id) {
        return Err(invalid_cluster(&cluster_id));
    }
    match state.managed_scaling_policies.get(&cluster_id) {
        Some(p) if !p.is_null() => Ok(json!({"ManagedScalingPolicy": p})),
        _ => Ok(json!({})),
    }
}

fn remove_managed_scaling_policy(state: &mut EmrState, body: &Value) -> OpResult {
    let cluster_id = as_str(param(body, "ClusterId")).unwrap_or_default();
    if !state.clusters.contains_key(&cluster_id) {
        return Err(invalid_cluster(&cluster_id));
    }
    state.managed_scaling_policies.remove(&cluster_id);
    Ok(json!({}))
}

fn put_auto_termination_policy(state: &mut EmrState, body: &Value) -> OpResult {
    let cluster_id = as_str(param(body, "ClusterId")).unwrap_or_default();
    if !state.clusters.contains_key(&cluster_id) {
        return Err(invalid_cluster(&cluster_id));
    }
    state.auto_termination_policies.insert(
        cluster_id,
        param(body, "AutoTerminationPolicy")
            .cloned()
            .unwrap_or(json!({})),
    );
    Ok(json!({}))
}

fn get_auto_termination_policy(state: &mut EmrState, body: &Value) -> OpResult {
    let cluster_id = as_str(param(body, "ClusterId")).unwrap_or_default();
    if !state.clusters.contains_key(&cluster_id) {
        return Err(invalid_cluster(&cluster_id));
    }
    match state.auto_termination_policies.get(&cluster_id) {
        Some(p) if !p.is_null() && p.as_object().map(|m| !m.is_empty()).unwrap_or(false) => {
            Ok(json!({"AutoTerminationPolicy": p}))
        }
        _ => Ok(json!({})),
    }
}

fn remove_auto_termination_policy(state: &mut EmrState, body: &Value) -> OpResult {
    let cluster_id = as_str(param(body, "ClusterId")).unwrap_or_default();
    if !state.clusters.contains_key(&cluster_id) {
        return Err(invalid_cluster(&cluster_id));
    }
    state.auto_termination_policies.remove(&cluster_id);
    Ok(json!({}))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::HashMap;

    const ACCOUNT: u64 = 123456789012;
    const REGION: &str = "us-east-1";

    fn run(op: &str, body: Value) -> Response {
        let req = Request {
            service: "emr".to_string(),
            operation: op.to_string(),
            method: "POST".to_string(),
            path: "/".to_string(),
            query_string: String::new(),
            headers: HashMap::new(),
            body: serde_json::to_vec(&body).unwrap_or_default().into(),
            account: ACCOUNT,
            region: REGION.to_string(),
        };
        dispatch(&req)
    }

    fn body_of(resp: &Response) -> Value {
        serde_json::from_slice(&resp.body).unwrap_or(Value::Null)
    }

    /// moto-crash rows render the werkzeug unhandled-exception page: the
    /// HTML body yields no parsable JSON for a boto3 client.
    fn assert_moto_crash(resp: &Response) {
        assert_eq!(resp.status, 500);
        assert!(
            resp.headers
                .iter()
                .any(|(k, v)| k == "Content-Type" && v == "text/html; charset=utf-8"),
            "{:?}",
            resp.headers
        );
        let text = String::from_utf8(resp.body.clone()).expect("utf8");
        assert!(text.starts_with("<!doctype html>"), "{text}");
        assert!(
            text.contains("<title>500 Internal Server Error</title>"),
            "{text}"
        );
    }

    fn status_of(resp: &Response) -> u16 {
        resp.status
    }

    fn cluster_fixture(name: &str) -> String {
        let resp = run(
            "RunJobFlow",
            json!({
                "Name": name,
                "ReleaseLabel": "emr-6.10.0",
                "Instances": {
                    "MasterInstanceType": "m5.xlarge",
                    "SlaveInstanceType": "m5.xlarge",
                    "InstanceCount": 1,
                    "KeepJobFlowAliveWhenNoSteps": true,
                },
                "JobFlowRole": "EMR_EC2_DefaultRole",
                "ServiceRole": "EMR_DefaultRole",
            }),
        );
        assert_eq!(status_of(&resp), 200);
        body_of(&resp)["JobFlowId"].as_str().unwrap().to_string()
    }

    #[test]
    fn run_job_flow_mints_cluster() {
        let id = cluster_fixture("mint");
        assert!(id.starts_with("j-"));
        run("TerminateJobFlows", json!({"JobFlowIds": [id]}));
    }

    #[test]
    fn describe_cluster_shape_and_error() {
        let id = cluster_fixture("describe");
        let desc = body_of(&run("DescribeCluster", json!({"ClusterId": id})));
        let cluster = desc["Cluster"].as_object().unwrap();
        assert_eq!(cluster["Id"], id);
        assert!(cluster.get("Name").is_some());
        assert!(cluster.get("Status").is_some());
        assert_eq!(cluster["ReleaseLabel"], "emr-6.10.0");
        assert_eq!(cluster["ServiceRole"], "EMR_DefaultRole");
        assert_eq!(cluster["TerminationProtected"], false);
        assert_eq!(cluster["AutoTerminate"], false);
        assert!(cluster["ClusterArn"]
            .as_str()
            .is_some_and(|a| a.starts_with("arn:aws:elasticmapreduce:")));
        assert_eq!(cluster["StepConcurrencyLevel"], 1);
        assert_eq!(
            cluster["Ec2InstanceAttributes"]["Ec2AvailabilityZone"],
            "us-east-1a"
        );
        assert_eq!(
            cluster["Ec2InstanceAttributes"]["IamInstanceProfile"],
            "EMR_EC2_DefaultRole"
        );
        assert_eq!(cluster["Applications"][0]["Args"], json!([]));
        assert_eq!(cluster["Applications"][0]["AdditionalInfo"], json!({}));
        assert!(cluster["Applications"][0]
            .get("Name")
            .is_none_or(|v| v.is_null()));

        let fake = run("DescribeCluster", json!({"ClusterId": "j-FAKE12345678"}));
        assert_eq!(status_of(&fake), 400);
        assert_eq!(body_of(&fake)["__type"], "InvalidRequestException");
        assert_eq!(body_of(&fake)["ErrorCode"], "NoSuchCluster");
        assert_eq!(
            body_of(&fake)["Message"],
            "Cluster id 'j-FAKE12345678' is not valid."
        );

        run("TerminateJobFlows", json!({"JobFlowIds": [id]}));
    }

    #[test]
    fn list_clusters_filters_and_summary() {
        let id = cluster_fixture("list-filters");
        let all = body_of(&run("ListClusters", json!({})));
        let clusters = all["Clusters"].as_array().unwrap().clone();
        assert!(clusters.iter().any(|c| c["Id"] == id));
        assert!(clusters.iter().all(|c| c.get("ClusterArn").is_some()));
        assert!(clusters.iter().all(|c| c.get("Status").is_some()));

        let filtered = body_of(&run(
            "ListClusters",
            json!({"ClusterStates": ["WAITING", "RUNNING", "STARTING"]}),
        ));
        assert!(filtered["Clusters"]
            .as_array()
            .unwrap()
            .iter()
            .any(|c| c["Id"] == id));

        let after = body_of(&run("ListClusters", json!({"CreatedAfter": 1577836800.0})));
        assert!(!after["Clusters"].as_array().unwrap().is_empty());

        run("TerminateJobFlows", json!({"JobFlowIds": [id]}));
    }

    #[test]
    fn tags_roundtrip_and_overwrite() {
        let id = cluster_fixture("tags");
        run(
            "AddTags",
            json!({"ResourceId": id, "Tags": [{"Key": "Env", "Value": "test"}, {"Key": "App", "Value": "x"}]}),
        );
        let cluster_tags = |resp: Response| {
            body_of(&resp)["Cluster"]["Tags"]
                .as_array()
                .cloned()
                .unwrap_or_default()
        };
        let first = cluster_tags(run("DescribeCluster", json!({"ClusterId": id})));
        assert!(first
            .iter()
            .any(|t| t["Key"] == "Env" && t["Value"] == "test"));

        run(
            "AddTags",
            json!({"ResourceId": id, "Tags": [{"Key": "Env", "Value": "prod"}]}),
        );
        let second = cluster_tags(run("DescribeCluster", json!({"ClusterId": id})));
        assert!(second
            .iter()
            .any(|t| t["Key"] == "Env" && t["Value"] == "prod"));
        assert!(!second
            .iter()
            .any(|t| t["Key"] == "Env" && t["Value"] == "test"));

        run("RemoveTags", json!({"ResourceId": id, "TagKeys": ["Env"]}));
        let third = cluster_tags(run("DescribeCluster", json!({"ClusterId": id})));
        assert!(!third.iter().any(|t| t["Key"] == "Env"));
        assert!(third.iter().any(|t| t["Key"] == "App"));

        run("TerminateJobFlows", json!({"JobFlowIds": [id]}));
    }

    #[test]
    fn terminate_and_visibility() {
        let id = cluster_fixture("terminate-visibility");

        run(
            "SetTerminationProtection",
            json!({"JobFlowIds": [id], "TerminationProtected": true}),
        );
        let desc = body_of(&run("DescribeCluster", json!({"ClusterId": id})));
        assert_eq!(desc["Cluster"]["TerminationProtected"], true);

        // moto: bare dict read on unknown cluster → moto's werkzeug HTML 500
        let bad = run(
            "SetTerminationProtection",
            json!({"JobFlowIds": ["j-NOPE123"]}),
        );
        assert_eq!(status_of(&bad), 500);
        assert_moto_crash(&bad);

        let bad_visible = run("SetVisibleToAllUsers", json!({"JobFlowIds": ["j-NOPE123"]}));
        assert_eq!(status_of(&bad_visible), 500);
        assert_moto_crash(&bad_visible);
        let _bad_term = run("TerminateJobFlows", json!({"JobFlowIds": ["j-NOPE123"]}));
        assert_eq!(status_of(&bad), 500);
        assert_eq!(status_of(&bad_visible), 500);

        run(
            "SetTerminationProtection",
            json!({"JobFlowIds": [id], "TerminationProtected": false}),
        );
        run(
            "SetVisibleToAllUsers",
            json!({"JobFlowIds": [id], "VisibleToAllUsers": true}),
        );
        let desc2 = body_of(&run("DescribeCluster", json!({"ClusterId": id})));
        assert_eq!(desc2["Cluster"]["VisibleToAllUsers"], true);

        run("TerminateJobFlows", json!({"JobFlowIds": [id]}));
        let desc3 = body_of(&run("DescribeCluster", json!({"ClusterId": id})));
        let state = desc3["Cluster"]["Status"]["State"].as_str().unwrap();
        assert!(matches!(state, "TERMINATING" | "TERMINATED"));
    }

    #[test]
    fn modify_cluster_step_concurrency() {
        let id = cluster_fixture("modify");
        let m = body_of(&run(
            "ModifyCluster",
            json!({"ClusterId": id, "StepConcurrencyLevel": 2}),
        ));
        assert_eq!(m["StepConcurrencyLevel"], 2);
        let fake = run(
            "ModifyCluster",
            json!({"ClusterId": "j-NOPE123", "StepConcurrencyLevel": 3}),
        );
        assert_eq!(body_of(&fake)["__type"], "InvalidRequestException");
        assert_eq!(body_of(&fake)["ErrorCode"], "NoSuchCluster");
        run("TerminateJobFlows", json!({"JobFlowIds": [id]}));
    }

    #[test]
    fn describe_job_flows_legacy_shape() {
        let id = cluster_fixture("djf");
        let djf = body_of(&run("DescribeJobFlows", json!({"JobFlowIds": [id]})));
        let flows = djf["JobFlows"].as_array().unwrap().clone();
        assert_eq!(flows.len(), 1);
        assert_eq!(flows[0]["JobFlowId"], id);
        assert!(flows[0].get("Name").is_some());
        assert!(flows[0].get("ExecutionStatusDetail").is_some());
        assert!(flows[0]["Instances"].get("Placement").is_some());
        assert!(flows[0]["Instances"]["KeepJobFlowAliveWhenNoSteps"].is_boolean());

        let empty = body_of(&run(
            "DescribeJobFlows",
            json!({"JobFlowIds": ["j-FAKE12345678"]}),
        ));
        assert_eq!(empty["JobFlows"].as_array().unwrap().len(), 0);
        run("TerminateJobFlows", json!({"JobFlowIds": [id]}));
    }

    #[test]
    fn run_job_flow_with_inline_details() {
        let created = run(
            "RunJobFlow",
            json!({
                "Name": "full-cluster",
                "ReleaseLabel": "emr-6.10.0",
                "Instances": {
                    "MasterInstanceType": "m5.xlarge",
                    "SlaveInstanceType": "m5.xlarge",
                    "InstanceCount": 1,
                    "KeepJobFlowAliveWhenNoSteps": true,
                },
                "JobFlowRole": "EMR_EC2_DefaultRole",
                "ServiceRole": "EMR_DefaultRole",
                "Tags": [{"Key": "App", "Value": "robotocore"}],
                "Configurations": [{"Classification": "spark-defaults", "Properties": {"spark.executor.memory": "2g"}}],
            }),
        );
        let id = body_of(&created)["JobFlowId"].as_str().unwrap().to_string();

        let desc = body_of(&run("DescribeCluster", json!({"ClusterId": id})))["Cluster"].clone();
        assert_eq!(
            desc["Configurations"][0]["Classification"],
            "spark-defaults"
        );
        assert_eq!(
            desc["Configurations"][0]["Properties"],
            json!({"spark.executor.memory": "2g"})
        );
        // default application: Name/Version are dropped by the moto shape pick
        assert!(!desc["Applications"][0]
            .as_object()
            .unwrap()
            .contains_key("Name"));
        assert!(desc["Applications"][0].get("Args").is_some());
        run("TerminateJobFlows", json!({"JobFlowIds": [id]}));
    }

    #[test]
    fn run_job_flow_rejects_ami_and_release() {
        let resp = run(
            "RunJobFlow",
            json!({
                "Name": "bad",
                "ReleaseLabel": "emr-6.10.0",
                "AmiVersion": "3.0",
                "Instances": {"MasterInstanceType": "m5.xlarge", "InstanceCount": 1},
                "JobFlowRole": "EMR_EC2_DefaultRole",
                "ServiceRole": "EMR_DefaultRole",
            }),
        );
        assert_eq!(status_of(&resp), 400);
        assert_eq!(body_of(&resp)["__type"], "ValidationException");
        assert_eq!(
            body_of(&resp)["message"],
            "Only one AMI version and release label may be specified. Provided AMI: 3.0, release label: emr-6.10.0."
        );
    }

    #[test]
    fn steps_add_describe_cancel() {
        let id = cluster_fixture("steps");
        let add = body_of(&run(
            "AddJobFlowSteps",
            json!({"JobFlowId": id, "Steps": [
                {"Name": "step-one", "ActionOnFailure": "CONTINUE", "HadoopJarStep": {"Jar": "command-runner.jar", "Args": ["echo", "hello"]}},
                {"Name": "step-two", "ActionOnFailure": "CONTINUE", "HadoopJarStep": {"Jar": "command-runner.jar", "Args": ["echo", "x"]}},
            ]}),
        ));
        let ids = add["StepIds"].as_array().unwrap().clone();
        assert_eq!(ids.len(), 2);

        let listed = body_of(&run("ListSteps", json!({"ClusterId": id})));
        let listed = listed["Steps"].as_array().unwrap().clone();
        assert_eq!(listed.len(), 2);
        assert_eq!(listed[0]["Name"], "step-two"); // newest first
        assert_eq!(listed[0]["Status"]["State"], "PENDING");
        assert_eq!(listed[1]["Status"]["State"], "RUNNING");
        assert_eq!(listed[1]["Config"]["Jar"], "command-runner.jar");
        assert_eq!(listed[1]["Config"]["Args"], json!(["echo", "hello"]));

        let first = ids[0].as_str().unwrap();
        let described = body_of(&run(
            "DescribeStep",
            json!({"ClusterId": id, "StepId": first}),
        ));
        assert_eq!(described["Step"]["Id"], first);
        assert_eq!(described["Step"]["Name"], "step-one");

        // custom jar + args preserved
        let custom = body_of(&run(
            "AddJobFlowSteps",
            json!({"JobFlowId": id, "Steps": [{"Name": "custom-jar", "ActionOnFailure": "CONTINUE", "HadoopJarStep": {"Jar": "s3://b/j.jar", "Args": ["a1", "a2"]}}]}),
        ));
        let cid = custom["StepIds"][0].as_str().unwrap().to_string();
        let described = body_of(&run(
            "DescribeStep",
            json!({"ClusterId": id, "StepId": cid}),
        ));
        assert_eq!(described["Step"]["Config"]["Jar"], "s3://b/j.jar");
        assert_eq!(described["Step"]["Config"]["Args"], json!(["a1", "a2"]));

        let cancelled = body_of(&run(
            "CancelSteps",
            json!({"ClusterId": id, "StepIds": [first, "s-FAKE12345678"]}),
        ));
        let list = cancelled["CancelStepsInfoList"].as_array().unwrap().clone();
        assert_eq!(list[0]["StepId"], first);
        assert_eq!(list[0]["Status"], "SUCCESS");
        assert_eq!(list[1]["Status"], "FAILED");
        assert_eq!(list[1]["Reason"], "Step s-FAKE12345678 is not found.");
        // cancelled step cannot be cancelled again
        let again = body_of(&run(
            "CancelSteps",
            json!({"ClusterId": id, "StepIds": [first]}),
        ));
        assert_eq!(again["CancelStepsInfoList"][0]["Status"], "FAILED");

        // unknown step in a real cluster → {}
        let missing = body_of(&run(
            "DescribeStep",
            json!({"ClusterId": id, "StepId": "s-MISSING"}),
        ));
        assert_eq!(missing, json!({}));

        // unknown cluster → NoSuchCluster
        let err = run(
            "AddJobFlowSteps",
            json!({"JobFlowId": "j-NOPE123", "Steps": []}),
        );
        assert_eq!(body_of(&err)["__type"], "InvalidRequestException");
        run("TerminateJobFlows", json!({"JobFlowIds": [id]}));
    }

    #[test]
    fn instance_groups_flow() {
        let id = cluster_fixture("groups");
        let before = body_of(&run("ListInstanceGroups", json!({"ClusterId": id})));
        let groups = before["InstanceGroups"].as_array().unwrap().clone();
        assert_eq!(groups.len(), 2); // master + slave auto-created
        assert!(groups.iter().any(|g| g["InstanceGroupType"] == "MASTER"));
        assert!(groups.iter().any(|g| g["InstanceGroupType"] == "CORE"));
        assert!(groups.iter().all(|g| g["Status"]["State"] == "RUNNING"));

        // add a TASK group
        run(
            "AddInstanceGroups",
            json!({"JobFlowId": id, "InstanceGroups": [{"Name": "task-group", "InstanceRole": "TASK", "InstanceType": "m5.xlarge", "InstanceCount": 1}]}),
        );
        let after = body_of(&run("ListInstanceGroups", json!({"ClusterId": id})));
        let names: Vec<&str> = after["InstanceGroups"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|g| g["Name"].as_str())
            .collect();
        assert!(names.contains(&"task-group"));
        assert!(groups
            .iter()
            .any(|g| !g.get("Id").is_none_or(|v| v.is_null())));

        // fake cluster add → NoSuchCluster
        let err = run(
            "AddInstanceGroups",
            json!({"JobFlowId": "j-NOPE123", "InstanceGroups": []}),
        );
        assert_eq!(body_of(&err)["ErrorCode"], "NoSuchCluster");

        run("TerminateJobFlows", json!({"JobFlowIds": [id]}));
    }

    #[test]
    fn modify_instance_groups_resize() {
        let id = cluster_fixture("resize");
        let before = body_of(&run("ListInstanceGroups", json!({"ClusterId": id})));
        let groups = before["InstanceGroups"].as_array().unwrap().clone();
        assert!(!groups.is_empty());
        let gid = groups[0]["Id"].as_str().unwrap().to_string();

        let resp = run(
            "ModifyInstanceGroups",
            json!({"InstanceGroups": [{"InstanceGroupId": gid, "InstanceCount": 2}]}),
        );
        assert_eq!(status_of(&resp), 200);
        let after = body_of(&run("ListInstanceGroups", json!({"ClusterId": id})));
        let target = after["InstanceGroups"]
            .as_array()
            .unwrap()
            .iter()
            .find(|g| g["Id"] == gid)
            .unwrap();
        assert_eq!(target["RequestedInstanceCount"], 2);

        // no args → 200 {}
        let empty = run("ModifyInstanceGroups", json!({}));
        assert_eq!(status_of(&empty), 200);
        assert_eq!(body_of(&empty), json!({}));

        // unknown group → moto KeyError crash (werkzeug HTML 500)
        let bad = run(
            "ModifyInstanceGroups",
            json!({"InstanceGroups": [{"InstanceGroupId": "i-NOPE", "InstanceCount": 2}]}),
        );
        assert_eq!(status_of(&bad), 500);
        assert_moto_crash(&bad);

        run("TerminateJobFlows", json!({"JobFlowIds": [id]}));
    }

    #[test]
    fn bootstrap_actions_listing() {
        let id = cluster_fixture("bootstrap");
        let resp = run(
            "RunJobFlow",
            json!({
                "Name": "bootstrap-cluster",
                "ReleaseLabel": "emr-6.10.0",
                "Instances": {"MasterInstanceType": "m5.xlarge", "InstanceCount": 1, "KeepJobFlowAliveWhenNoSteps": true},
                "JobFlowRole": "EMR_EC2_DefaultRole",
                "ServiceRole": "EMR_DefaultRole",
                "BootstrapActions": [{"Name": "install-deps", "ScriptBootstrapAction": {"Path": "s3://b/b.sh", "Args": ["--v"]}}],
            }),
        );
        let cid = body_of(&resp)["JobFlowId"].as_str().unwrap().to_string();
        let listed = body_of(&run("ListBootstrapActions", json!({"ClusterId": cid})));
        let actions = listed["BootstrapActions"].as_array().unwrap().clone();
        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0]["Name"], "install-deps");
        assert_eq!(actions[0]["ScriptPath"], "s3://b/b.sh");
        assert_eq!(actions[0]["Args"], json!(["--v"]));
        run("TerminateJobFlows", json!({"JobFlowIds": [cid]}));
        run("TerminateJobFlows", json!({"JobFlowIds": [id]}));
    }

    #[test]
    fn instance_fleets_flow() {
        let created = run(
            "RunJobFlow",
            json!({
                "Name": "fleet-cluster",
                "ReleaseLabel": "emr-6.10.0",
                "Instances": {
                    "InstanceFleets": [{"Name": "master-fleet", "InstanceFleetType": "MASTER", "TargetOnDemandCapacity": 1, "InstanceTypeConfigs": [{"InstanceType": "m5.xlarge"}]}],
                    "KeepJobFlowAliveWhenNoSteps": true,
                },
                "JobFlowRole": "EMR_EC2_DefaultRole",
                "ServiceRole": "EMR_DefaultRole",
            }),
        );
        let cid = body_of(&created)["JobFlowId"].as_str().unwrap().to_string();
        // RunJobFlow ignores Instances.InstanceFleets (moto parity)
        let empty = body_of(&run("ListInstanceFleets", json!({"ClusterId": cid})));
        assert_eq!(empty["InstanceFleets"].as_array().unwrap().len(), 0);

        let added = body_of(&run(
            "AddInstanceFleet",
            json!({"ClusterId": cid, "InstanceFleet": {"Name": "task-fleet", "InstanceFleetType": "TASK", "TargetOnDemandCapacity": 1, "InstanceTypeConfigs": [{"InstanceType": "m5.xlarge"}]}}),
        ));
        assert_eq!(added["ClusterId"], cid);
        assert!(added["InstanceFleetId"]
            .as_str()
            .unwrap()
            .starts_with("if-"));
        let fid = added["InstanceFleetId"].as_str().unwrap().to_string();

        let listed = body_of(&run("ListInstanceFleets", json!({"ClusterId": cid})));
        let fleets = listed["InstanceFleets"].as_array().unwrap().clone();
        assert_eq!(fleets.len(), 1);
        assert_eq!(fleets[0]["Name"], "task-fleet");
        assert_eq!(fleets[0]["InstanceFleetType"], "TASK");
        assert_eq!(
            fleets[0]["InstanceTypeSpecifications"][0]["InstanceType"],
            "m5.xlarge"
        );
        assert_eq!(
            fleets[0]["Status"]["StateChangeReason"]["Code"],
            "USER_REQUEST"
        );

        let modified = run(
            "ModifyInstanceFleet",
            json!({"ClusterId": cid, "InstanceFleet": {"InstanceFleetId": fid, "TargetOnDemandCapacity": 2}}),
        );
        assert_eq!(status_of(&modified), 200);
        let listed2 = body_of(&run("ListInstanceFleets", json!({"ClusterId": cid})));
        assert_eq!(listed2["InstanceFleets"][0]["TargetOnDemandCapacity"], 2);
        assert_eq!(
            listed2["InstanceFleets"][0]["ProvisionedOnDemandCapacity"],
            2
        );

        let unknown = run(
            "ModifyInstanceFleet",
            json!({"ClusterId": cid, "InstanceFleet": {"InstanceFleetId": "if-NOPEXXXYYY", "TargetOnDemandCapacity": 3}}),
        );
        assert_eq!(status_of(&unknown), 400);
        assert_eq!(body_of(&unknown)["ErrorCode"], "ResourceNotFound");

        run("TerminateJobFlows", json!({"JobFlowIds": [cid]}));
    }

    #[test]
    fn auto_scaling_policy_roundtrip() {
        let id = cluster_fixture("scaling");
        let before = body_of(&run("ListInstanceGroups", json!({"ClusterId": id})));
        let groups = before["InstanceGroups"].as_array().unwrap().clone();
        let gid = groups[0]["Id"].as_str().unwrap().to_string();

        let attached = body_of(&run(
            "PutAutoScalingPolicy",
            json!({"ClusterId": id, "InstanceGroupId": gid, "AutoScalingPolicy": {
                "Constraints": {"MinCapacity": 1, "MaxCapacity": 5},
                "Rules": [{"Name": "scale-out", "Action": {"SimpleScalingPolicyConfiguration": {"ScalingAdjustment": 1, "AdjustmentType": "CHANGE_IN_CAPACITY"}}}],
            }}),
        ));
        let policy = &attached["AutoScalingPolicy"];
        assert_eq!(policy["Status"]["State"], "ATTACHED");
        assert_eq!(policy["Constraints"]["MinCapacity"], 1);
        assert_eq!(
            policy["Rules"][0]["Action"]["SimpleScalingPolicyConfiguration"]["ScalingAdjustment"],
            1
        );
        assert_eq!(attached["ClusterId"], id);
        assert_eq!(attached["InstanceGroupId"], gid);

        let list_after = body_of(&run("ListInstanceGroups", json!({"ClusterId": id})));
        let group = list_after["InstanceGroups"]
            .as_array()
            .unwrap()
            .iter()
            .find(|g| g["Id"] == gid)
            .unwrap();
        assert_eq!(group["AutoScalingPolicy"]["Status"]["State"], "ATTACHED");

        let removed = run(
            "RemoveAutoScalingPolicy",
            json!({"ClusterId": id, "InstanceGroupId": gid}),
        );
        assert_eq!(status_of(&removed), 200);
        let listed = body_of(&run("ListInstanceGroups", json!({"ClusterId": id})));
        let group2 = listed["InstanceGroups"]
            .as_array()
            .unwrap()
            .iter()
            .find(|g| g["Id"] == gid)
            .unwrap();
        assert!(group2.get("AutoScalingPolicy").is_none());

        // unknown group → moto assert → the same werkzeug HTML 500 page
        let bad = run(
            "PutAutoScalingPolicy",
            json!({"ClusterId": id, "InstanceGroupId": "i-NOPE123", "AutoScalingPolicy": {}}),
        );
        assert_eq!(status_of(&bad), 500);
        assert_moto_crash(&bad);

        run("TerminateJobFlows", json!({"JobFlowIds": [id]}));
    }

    #[test]
    fn snake_to_pascal_roundtrip() {
        assert_eq!(
            snake_to_pascal("simple_scaling_policy_configuration"),
            "SimpleScalingPolicyConfiguration"
        );
        assert_eq!(
            pascal_to_snake("SimpleScalingPolicyConfiguration"),
            "simple_scaling_policy_configuration"
        );
        assert_eq!(xform("MasterInstanceType"), "master_instance_type");
    }

    #[test]
    fn security_configuration_lifecycle() {
        let created = body_of(&run(
            "CreateSecurityConfiguration",
            json!({"Name": "sc-test", "SecurityConfiguration": json!({"EncryptionConfiguration": {"EnableInTransitEncryption": false}}).to_string()}),
        ));
        assert_eq!(created["Name"], "sc-test");
        assert_eq!(created["Name"], "sc-test");
        assert!(created.get("CreationDateTime").is_some());

        let described = body_of(&run(
            "DescribeSecurityConfiguration",
            json!({"Name": "sc-test"}),
        ));
        assert_eq!(described["Name"], "sc-test");
        assert_eq!(
            described["SecurityConfiguration"],
            json!({"EncryptionConfiguration": {"EnableInTransitEncryption": false}}).to_string()
        );

        let listed = body_of(&run("ListSecurityConfigurations", json!({})));
        assert!(listed["SecurityConfigurations"]
            .as_array()
            .unwrap()
            .iter()
            .any(|c| c["Name"] == "sc-test"));

        // duplicate → moto's keyword-message InvalidRequestException
        let dup = run(
            "CreateSecurityConfiguration",
            json!({"Name": "sc-test", "SecurityConfiguration": "{}"}),
        );
        assert_eq!(status_of(&dup), 400);
        assert_eq!(body_of(&dup)["__type"], "InvalidRequestException");
        assert_eq!(
            body_of(&dup)["Message"],
            "An unspecified service error occurred"
        );

        let deleted = run("DeleteSecurityConfiguration", json!({"Name": "sc-test"}));
        assert_eq!(status_of(&deleted), 200);

        let missing = run("DescribeSecurityConfiguration", json!({"Name": "sc-test"}));
        assert_eq!(status_of(&missing), 400);
        assert_eq!(body_of(&missing)["ErrorCode"], "ResourceNotFound");

        let missing_del = run("DeleteSecurityConfiguration", json!({"Name": "sc-test"}));
        assert_eq!(status_of(&missing_del), 400);
        assert_eq!(body_of(&missing_del)["ErrorCode"], "ResourceNotFound");
    }

    #[test]
    fn block_public_access_roundtrip() {
        let empty = body_of(&run("GetBlockPublicAccessConfiguration", json!({})));
        // never errors — unset store renders empty members
        assert_eq!(empty["BlockPublicAccessConfiguration"], json!({}));
        assert_eq!(empty["BlockPublicAccessConfigurationMetadata"], json!({}));

        run(
            "PutBlockPublicAccessConfiguration",
            json!({"BlockPublicAccessConfiguration": {"BlockPublicSecurityGroupRules": true, "PermittedPublicSecurityGroupRuleRanges": []}}),
        );
        let got = body_of(&run("GetBlockPublicAccessConfiguration", json!({})));
        assert_eq!(
            got["BlockPublicAccessConfiguration"]["BlockPublicSecurityGroupRules"],
            true
        );
        assert_eq!(
            got["BlockPublicAccessConfiguration"]["PermittedPublicSecurityGroupRuleRanges"],
            json!([])
        );
        assert!(got["BlockPublicAccessConfigurationMetadata"]
            .get("CreationDateTime")
            .is_some());
        assert_eq!(
            got["BlockPublicAccessConfigurationMetadata"]["CreatedByArn"],
            "arn:aws:sts::123456789012:user/moto"
        );
    }

    #[test]
    fn studio_crud() {
        let created = body_of(&run(
            "CreateStudio",
            json!({
                "Name": "studio-a",
                "AuthMode": "IAM",
                "VpcId": "vpc-1",
                "SubnetIds": ["subnet-1"],
                "ServiceRole": "role",
                "WorkspaceSecurityGroupId": "sg-1",
                "EngineSecurityGroupId": "sg-2",
                "DefaultS3Location": "s3://b/st/",
            }),
        ));
        let sid = created["StudioId"].as_str().unwrap().to_string();
        assert!(sid.starts_with("es-"));
        assert_eq!(
            created["Url"],
            format!("https://{sid}.emrstudio-prod.us-east-1.amazonaws.com")
        );

        let described = body_of(&run("DescribeStudio", json!({"StudioId": sid})))["Studio"].clone();
        assert_eq!(described["StudioId"], sid);
        assert_eq!(described["Name"], "studio-a");
        assert_eq!(
            described["StudioArn"],
            format!("arn:aws:elasticmapreduce:us-east-1:123456789012:studio/{sid}")
        );
        assert_eq!(described["SubnetIds"], json!(["subnet-1"]));

        let listed = body_of(&run("ListStudios", json!({})));
        assert!(listed["Studios"]
            .as_array()
            .unwrap()
            .iter()
            .any(|s| s["StudioId"] == sid));

        run("DeleteStudio", json!({"StudioId": sid}));
        let missing = run("DescribeStudio", json!({"StudioId": sid}));
        assert_eq!(status_of(&missing), 400);
        assert_eq!(body_of(&missing)["__type"], "InvalidRequestException");
        assert_eq!(
            body_of(&missing)["Message"],
            "An unspecified service error occurred"
        );
    }

    #[test]
    fn studio_session_mapping_key_quirk() {
        let created = body_of(&run(
            "CreateStudio",
            json!({"Name": "studio-b", "AuthMode": "IAM", "VpcId": "vpc", "SubnetIds": ["sn"], "ServiceRole": "r", "WorkspaceSecurityGroupId": "sg", "EngineSecurityGroupId": "sg"}),
        ));
        let sid = created["StudioId"].as_str().unwrap().to_string();

        // create with IdentityName (identity_id defaults to "")
        run(
            "CreateStudioSessionMapping",
            json!({"StudioId": sid, "IdentityType": "USER", "IdentityName": "user@x.com", "SessionPolicyArn": "arn:p"}),
        );

        // lookup with absent IdentityId → moto f-string "None" → miss
        let miss = run(
            "GetStudioSessionMapping",
            json!({"StudioId": sid, "IdentityType": "USER", "IdentityName": "nobody@x.com"}),
        );
        assert_eq!(status_of(&miss), 400);
        assert_eq!(body_of(&miss)["__type"], "InvalidRequestException");

        let upd = run(
            "UpdateStudioSessionMapping",
            json!({"StudioId": sid, "IdentityType": "USER", "IdentityName": "nobody@x.com", "SessionPolicyArn": "arn:x"}),
        );
        assert_eq!(status_of(&upd), 400);

        // create with explicit IdentityId → lookup hits
        run(
            "CreateStudioSessionMapping",
            json!({"StudioId": sid, "IdentityType": "USER", "IdentityId": "id-1", "SessionPolicyArn": "arn:p"}),
        );
        let found = body_of(&run(
            "GetStudioSessionMapping",
            json!({"StudioId": sid, "IdentityType": "USER", "IdentityId": "id-1"}),
        ));
        assert_eq!(found["SessionMapping"]["IdentityId"], "id-1");
        assert_eq!(found["SessionMapping"]["SessionPolicyArn"], "arn:p");

        let del = run(
            "DeleteStudioSessionMapping",
            json!({"StudioId": sid, "IdentityType": "USER", "IdentityId": "id-1"}),
        );
        assert_eq!(status_of(&del), 200);
        let remiss = run(
            "GetStudioSessionMapping",
            json!({"StudioId": sid, "IdentityType": "USER", "IdentityId": "id-1"}),
        );
        assert_eq!(status_of(&remiss), 400);

        // unknown studio → generic invalid
        let bad = run(
            "CreateStudioSessionMapping",
            json!({"StudioId": "es-NOPE123", "IdentityType": "USER", "IdentityName": "x"}),
        );
        assert_eq!(body_of(&bad)["__type"], "InvalidRequestException");
    }

    #[test]
    fn notebook_and_persistent_app_ui() {
        // StartNotebookExecution creates an execution (moto backend under the
        // patched-spec action name; no cluster validation).
        let started = body_of(&run(
            "StartNotebookExecution",
            json!({
                "ExecutionEngine": {"Id": "j-FAKE123", "Type": "EMR"},
                "ServiceRole": "arn:aws:iam::123456789012:role/EMR_Notebooks_DefaultRole",
            }),
        ));
        let nid = started["NotebookExecutionId"].as_str().unwrap().to_string();
        assert!(nid.starts_with("ex-"));

        let described = body_of(&run(
            "DescribeNotebookExecution",
            json!({"NotebookExecutionId": nid}),
        ));
        assert_eq!(described["NotebookExecution"]["NotebookExecutionId"], nid);
        assert_eq!(described["NotebookExecution"]["Status"], "STARTING");

        // describe/stop of a nonexistent execution → generic InvalidRequest
        let miss_desc = run(
            "DescribeNotebookExecution",
            json!({"NotebookExecutionId": "ne-FAKE123"}),
        );
        assert_eq!(status_of(&miss_desc), 400);
        assert_eq!(body_of(&miss_desc)["__type"], "InvalidRequestException");
        let miss_stop = run(
            "StopNotebookExecution",
            json!({"NotebookExecutionId": "ne-FAKE123"}),
        );
        assert_eq!(status_of(&miss_stop), 400);

        let listed = body_of(&run(
            "ListNotebookExecutions",
            json!({"Status": "STARTING"}),
        ));
        let ids: Vec<&str> = listed["NotebookExecutions"]
            .as_array()
            .unwrap()
            .iter()
            .map(|e| e["NotebookExecutionId"].as_str().unwrap())
            .collect();
        assert!(ids.contains(&nid.as_str()));

        let ui = body_of(&run(
            "CreatePersistentAppUI",
            json!({"TargetResourceArn": "arn:aws:emr:us-east-1:123456789012:cluster/j-1"}),
        ));
        let ui_id = ui["PersistentAppUIId"].as_str().unwrap().to_string();
        assert!(!ui_id.is_empty());
        assert!(ui.get("PersistentAppUI").is_none()); // patched-spec drops it

        let described = body_of(&run(
            "DescribePersistentAppUI",
            json!({"PersistentAppUIId": ui_id}),
        ));
        assert_eq!(
            described["PersistentAppUI"]["PersistentAppUIId"],
            ui["PersistentAppUIId"]
        );
        assert_eq!(
            described["PersistentAppUI"]["PersistentAppUIStatus"],
            "ACTIVE"
        );

        let miss_ui = run(
            "DescribePersistentAppUI",
            json!({"PersistentAppUIId": "nope"}),
        );
        assert_eq!(status_of(&miss_ui), 400);
        assert_eq!(body_of(&miss_ui)["ErrorCode"], "ResourceNotFound");
        assert_eq!(
            body_of(&miss_ui)["Message"],
            "PersistentAppUI nope does not exist."
        );
    }

    #[test]
    fn release_labels_and_supported_instance_types() {
        let labels = body_of(&run("ListReleaseLabels", json!({})));
        let all = labels["ReleaseLabels"].as_array().unwrap().clone();
        assert!(all.iter().any(|l| l == "emr-6.10.0"));

        let described = body_of(&run(
            "DescribeReleaseLabel",
            json!({"ReleaseLabel": "emr-6.10.0"}),
        ));
        assert_eq!(described["ReleaseLabel"], "emr-6.10.0");
        assert_eq!(described["Applications"], json!([]));
        assert_eq!(described["AvailableOSReleases"], json!([]));

        let miss = run(
            "DescribeReleaseLabel",
            json!({"ReleaseLabel": "emr-99.99.99"}),
        );
        assert_eq!(status_of(&miss), 400);
        assert_eq!(body_of(&miss)["__type"], "InvalidRequestException");

        let types = body_of(&run(
            "ListSupportedInstanceTypes",
            json!({"ReleaseLabel": "emr-6.10.0"}),
        ));
        let list = types["SupportedInstanceTypes"].as_array().unwrap();
        assert!(!list.is_empty());
        assert!(list.iter().all(|t| t.get("Type").is_some()));

        let unsupported = run(
            "ListSupportedInstanceTypes",
            json!({"ReleaseLabel": "emr-99.99.99"}),
        );
        assert_eq!(status_of(&unsupported), 400);
        assert_eq!(body_of(&unsupported)["__type"], "ValidationException");
        assert_eq!(
            body_of(&unsupported)["message"],
            "Release label emr-99.99.99 is not supported."
        );
    }

    #[test]
    fn scaling_and_termination_policies() {
        let id = cluster_fixture("policies");

        let put = run(
            "PutManagedScalingPolicy",
            json!({"ClusterId": id, "ManagedScalingPolicy": {"ComputeLimits": {"UnitType": "Instances", "MinimumCapacityUnits": 1, "MaximumCapacityUnits": 10}}}),
        );
        assert_eq!(status_of(&put), 200);
        assert_eq!(body_of(&put), json!({}));

        let got = body_of(&run("GetManagedScalingPolicy", json!({"ClusterId": id})));
        assert_eq!(
            got["ManagedScalingPolicy"]["ComputeLimits"]["UnitType"],
            "Instances"
        );
        assert_eq!(
            got["ManagedScalingPolicy"]["ComputeLimits"]["MaximumCapacityUnits"],
            10
        );

        run("RemoveManagedScalingPolicy", json!({"ClusterId": id}));
        let got_empty = body_of(&run("GetManagedScalingPolicy", json!({"ClusterId": id})));
        assert_eq!(got_empty, json!({}));

        run(
            "PutAutoTerminationPolicy",
            json!({"ClusterId": id, "AutoTerminationPolicy": {"IdleTimeout": 3600}}),
        );
        let got_term = body_of(&run("GetAutoTerminationPolicy", json!({"ClusterId": id})));
        assert_eq!(got_term["AutoTerminationPolicy"]["IdleTimeout"], 3600);
        run("RemoveAutoTerminationPolicy", json!({"ClusterId": id}));
        let got_term2 = body_of(&run("GetAutoTerminationPolicy", json!({"ClusterId": id})));
        assert_eq!(got_term2, json!({}));

        // fake cluster on each policy op → NoSuchCluster (moto describe_cluster)
        for op in [
            "PutManagedScalingPolicy",
            "GetManagedScalingPolicy",
            "RemoveManagedScalingPolicy",
            "PutAutoTerminationPolicy",
            "GetAutoTerminationPolicy",
            "RemoveAutoTerminationPolicy",
        ] {
            let err = run(op, json!({"ClusterId": "j-FAKE12345678"}));
            assert_eq!(body_of(&err)["__type"], "InvalidRequestException", "{op}");
            assert_eq!(body_of(&err)["ErrorCode"], "NoSuchCluster", "{op}");
        }

        run("TerminateJobFlows", json!({"JobFlowIds": [id]}));
    }

    #[test]
    fn update_studio_remains_deny_501() {
        let resp = run("UpdateStudio", json!({}));
        assert_eq!(status_of(&resp), 501);
        assert_eq!(body_of(&resp)["__type"], "NotImplemented");
        assert_eq!(
            body_of(&resp)["message"],
            "The update_studio action has not been implemented"
        );
    }

    #[test]
    fn gap_ops_now_implemented() {
        // ListStudioSessionMappings — rendering present even before any
        // mapping exists (may see mappings from parallel unit tests).
        let listed = body_of(&run("ListStudioSessionMappings", json!({})));
        assert!(listed["SessionMappings"].as_array().is_some());

        // SetKeepJobFlowAliveWhenNoSteps / SetUnhealthyNodeReplacement on a
        // fake job-flow id → 200 (suite-pinned), and the flag flips on a real
        // cluster.
        let flipped = run(
            "SetKeepJobFlowAliveWhenNoSteps",
            json!({"JobFlowIds": ["j-FAKE123456"], "KeepJobFlowAliveWhenNoSteps": true}),
        );
        assert_eq!(status_of(&flipped), 200);
        assert_eq!(body_of(&flipped), json!({}));
        let fake_replacement = run(
            "SetUnhealthyNodeReplacement",
            json!({"JobFlowIds": ["j-FAKE123456"], "UnhealthyNodeReplacement": true}),
        );
        assert_eq!(status_of(&fake_replacement), 200);

        let id = cluster_fixture("alive");
        let resp = run(
            "SetKeepJobFlowAliveWhenNoSteps",
            json!({"JobFlowIds": [id], "KeepJobFlowAliveWhenNoSteps": false}),
        );
        assert_eq!(status_of(&resp), 200);
        let described = body_of(&run("DescribeJobFlows", json!({"JobFlowIds": [id]})));
        assert_eq!(
            described["JobFlows"][0]["Instances"]["KeepJobFlowAliveWhenNoSteps"],
            false
        );
        run("TerminateJobFlows", json!({"JobFlowIds": [id]}));

        // GetClusterSessionCredentials → credentials struct + expiry.
        let creds = body_of(&run(
            "GetClusterSessionCredentials",
            json!({"ClusterId": "j-FAKE12345", "ExecutionRoleArn": "arn:aws:iam::123456789012:role/test-role"}),
        ));
        assert!(creds["Credentials"]["UsernamePassword"]["Username"].is_string());
        assert!(creds["Credentials"]["UsernamePassword"]["Password"].is_string());
        assert!(creds["ExpiresAt"].is_number());

        // GetPersistentAppUIPresignedURL (existing id) / unknown id error.
        let ui = body_of(&run(
            "CreatePersistentAppUI",
            json!({"TargetResourceArn": "arn:aws:emr:us-east-1:123456789012:cluster/j-1"}),
        ));
        let ui_id = ui["PersistentAppUIId"].as_str().unwrap().to_string();
        let url = body_of(&run(
            "GetPersistentAppUIPresignedURL",
            json!({"PersistentAppUIId": ui_id}),
        ));
        assert!(url["PresignedURL"].as_str().unwrap().contains(&ui_id));
        let miss_url = run(
            "GetPersistentAppUIPresignedURL",
            json!({"PersistentAppUIId": "nope"}),
        );
        assert_eq!(status_of(&miss_url), 400);
        assert_eq!(body_of(&miss_url)["ErrorCode"], "ResourceNotFound");

        // GetOnClusterAppUIPresignedURL → 200 even for a fake cluster id.
        let on_cluster = run(
            "GetOnClusterAppUIPresignedURL",
            json!({"ClusterId": "j-FAKE123", "OnClusterAppUIType": "SPARK_HISTORY_SERVER"}),
        );
        assert_eq!(status_of(&on_cluster), 200);
        assert!(body_of(&on_cluster)["PresignedURL"].is_string());
    }
}
