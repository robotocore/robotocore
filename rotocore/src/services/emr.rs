//! EMR backend (**JSON protocol**, `X-Amz-Target: ElasticMapReduce.<Op>`).
//!
//! The repo's botocore data pins EMR to the JSON wire (jsonVersion 1.1,
//! targetPrefix `ElasticMapReduce`), so both request bodies and responses are
//! JSON, not query-XML. Shapes, error codes and quirks mirror the moto `emr`
//! backend (which the compat suites pin):
//!
//! - Denied-by-default ops return moto's `NotImplemented` document
//!   (`The <snake_op> action has not been implemented`, HTTP 501). The
//!   suite-frozen gap list: ListStudioSessionMappings,
//!   SetKeepJobFlowAliveWhenNoSteps, SetUnhealthyNodeReplacement,
//!   GetClusterSessionCredentials, GetPersistentAppUIPresignedURL,
//!   GetOnClusterAppUIPresignedURL, StartNotebookExecution (plus UpdateStudio).
//! - Unknown clusters answer `InvalidRequestException`/`NoSuchCluster`; a few
//!   moto paths (bare `self.clusters[id]` dict reads) escape as KeyError →
//!   `InternalError` (SetTerminationProtection, SetVisibleToAllUsers,
//!   TerminateJobFlows, ModifyInstanceGroups).
//! - `Instances.InstanceFleets` on RunJobFlow is ignored — moto only creates
//!   fleets through `AddInstanceFleet`.
//! - State is per-(account, region) via [`crate::store`].

use crate::store;
use crate::{Request, Response};
use serde_json::{json, Map, Value};
use std::collections::HashMap;

// --- embedded release data (moto resources) — used by the release-label /
//     supported-instance-types handlers (volume-family commits) --------------

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

#[derive(Default)]
struct EmrState {
    clusters: HashMap<String, Cluster>,
    instance_groups: HashMap<String, InstanceGroup>,
    security_configurations: HashMap<String, SecurityConfiguration>,
    /// Store mirroring moto's
    /// `{"block_public_access_configuration": …, "block_public_access_configuration_metadata": …}`.
    block_public_access: Option<Value>,
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
/// `master_instance_type`).
fn xform(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 4);
    for (i, c) in s.chars().enumerate() {
        if c.is_ascii_uppercase() {
            if i > 0 {
                let prev = s.as_bytes()[i - 1];
                if prev.is_ascii_lowercase() || prev.is_ascii_digit() {
                    out.push('_');
                }
            }
            out.push(c.to_ascii_lowercase());
        } else {
            out.push(c);
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
    Response::json_status(
        400,
        &json!({"__type": "ValidationException", "Message": message}),
    )
}

/// moto's KeyError escape (e.g. `self.clusters[unknown_id]`).
fn key_error(key: &str) -> Response {
    Response::json_status(
        500,
        &json!({"__type": "InternalError", "message": format!("'{}'", key)}),
    )
}

fn ok(body: Value) -> Response {
    Response::json_status(200, &body)
}

// ---------------------------------------------------------------------------
// IDs / time
// ---------------------------------------------------------------------------

const ID_CHARS: &[u8] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";

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
                obj.insert("AutoScalingPolicy".into(), policy.clone());
            }
            obj.insert("EbsBlockDevices".into(), json!([]));
            Value::Object(obj)
        })
        .collect()
}

fn terminate_cluster(c: &mut Cluster) {
    c.state = Some("TERMINATING".into());
    c.end_at = Some(now_ts());
    c.state = Some("TERMINATED".into());
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

        // moto: bare dict read on unknown cluster → InternalError 500
        let bad = run(
            "SetTerminationProtection",
            json!({"JobFlowIds": ["j-NOPE123"]}),
        );
        assert_eq!(status_of(&bad), 500);
        assert_eq!(body_of(&bad)["__type"], "InternalError");
        assert_eq!(body_of(&bad)["message"], "'j-NOPE123'");

        let bad_visible = run("SetVisibleToAllUsers", json!({"JobFlowIds": ["j-NOPE123"]}));
        assert_eq!(status_of(&bad_visible), 500);
        let bad_term = run("TerminateJobFlows", json!({"JobFlowIds": ["j-NOPE123"]}));
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
            body_of(&resp)["Message"],
            "Only one AMI version and release label may be specified. Provided AMI: 3.0, release label: emr-6.10.0."
        );
    }
}
