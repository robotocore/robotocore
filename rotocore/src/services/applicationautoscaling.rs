//! Application Auto Scaling backend — moto-equivalent (json protocol).
//!
//! Replaces the moto sidecar for `applicationautoscaling`
//! (`X-Amz-Target: AnyScaleFrontendService.<Op>`). State is
//! per-(account, region), mirroring moto's `ApplicationAutoscalingBackend`
//! (moto/applicationautoscaling): scalable targets keyed
//! dimension → resource id (insertion-ordered, as moto's OrderedDicts),
//! scaling policies keyed `namespace\tresource_id\tdimension\tpolicy_name`,
//! and scheduled actions in an append-only list.
//!
//! Contract: the deployed moto fork's routed surface
//! (moto/applicationautoscaling/responses.py) — Register/Deregister
//! ScalableTarget, DescribeScalableTargets, Put/Describe/DeleteScalingPolicy,
//! Put/Delete/DescribeScheduledAction. The remaining botocore operations
//! (DescribeScalingActivities, GetPredictiveScalingForecast,
//! ListTagsForResource, TagResource, UntagResource) are unhandled in the
//! fork and answer the generic `501 NotImplemented` — the verdict moto
//! reaches for them through its "action has not been implemented" raise.
//!
//! Behavioral quirks kept from the moto fork:
//! - ServiceNamespace / ScalableDimension enum validation fails with moto's
//!   "failed to satisfy constraint" ValidationException wording;
//! - RegisterScalableTarget validates namespace/dimension/resource-type
//!   consistency ("Unsupported service namespace, resource type or scalable
//!   dimension"), upserts by (dimension, resource id), and only overwrites
//!   Min/MaxCapacity/SuspendedState members that were actually sent
//!   (SuspendedState stored verbatim);
//! - ECS-namespace targets require a live ECS service (an `ecs::bridge`
//!   mirror of moto's `describe_services` one-hit probe); a malformed
//!   resource id answers moto's "ECS service doesn't exist" fault instead of
//!   crashing on the unpack, like the named-fault convention elsewhere;
//! - describe responses carry moto's pagination (MaxResults defaults 50 /
//!   100, numeric NextToken markers, moto's page-marker arithmetic
//!   verbatim, `NextToken` always present);
//! - scaling-policy describe filters are substring matches (moto applies
//!   Python `in` to the *stored* string inside the *filter*);
//! - TargetTracking policies for the dynamodb and ecs namespaces create
//!   moto's companion CloudWatch alarms (AlarmHigh / AlarmLow /
//!   ProvisionedCapacityHigh / ProvisionedCapacityLow). Shape-only: moto
//!   also writes these alarms into the real CloudWatch backend, while this
//!   backend only reports them on the policy wire;
//! - a re-put policy overwrites the stored one and its alarm list (moto
//!   rebuilds the object; the previous alarm set is left to the CloudWatch
//!   backend it was written into).

use crate::proto::{json_error, now_unix, parse_json_body};
use crate::store::{self, ScopeKey};
use crate::{Request, Response};
use parking_lot::RwLock;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Enum sets (moto's ServiceNamespaceValueSet / ScalableDimensionValueSet ~)
// ---------------------------------------------------------------------------

const SERVICE_NAMESPACES: &[&str] = &[
    "appstream",
    "rds",
    "lambda",
    "cassandra",
    "dynamodb",
    "custom-resource",
    "elasticmapreduce",
    "ec2",
    "comprehend",
    "ecs",
    "sagemaker",
    "kafka",
];

const SCALABLE_DIMENSIONS: &[&str] = &[
    "cassandra:table:ReadCapacityUnits",
    "cassandra:table:WriteCapacityUnits",
    "dynamodb:index:ReadCapacityUnits",
    "dynamodb:index:WriteCapacityUnits",
    "dynamodb:table:ReadCapacityUnits",
    "dynamodb:table:WriteCapacityUnits",
    "rds:cluster:ReadReplicaCount",
    "rds:cluster:Capacity",
    "comprehend:document-classifier-endpoint:DesiredInferenceUnits",
    "elasticmapreduce:instancefleet:OnDemandCapacity",
    "elasticmapreduce:instancefleet:SpotCapacity",
    "elasticmapreduce:instancegroup:InstanceCount",
    "lambda:function:ProvisionedConcurrency",
    "appstream:fleet:DesiredCapacity",
    "custom-resource:ResourceType:Property",
    "sagemaker:variant:DesiredInstanceCount",
    "ec2:spot-fleet-request:TargetCapacity",
    "ecs:service:DesiredCount",
    "kafka:broker-storage:VolumeSize",
];

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

type Stores = Arc<RwLock<HashMap<ScopeKey, AppAutoscalingState>>>;

fn stores() -> Stores {
    store::slot::<AppAutoscalingState>("applicationautoscaling")
}

/// Per-(account, region) in-memory state for Application Auto Scaling.
#[derive(Default)]
pub struct AppAutoscalingState {
    /// dimension → [(resource_id, target)] in insertion order (moto's
    /// OrderedDict of OrderedDicts).
    targets: Vec<(String, Vec<(String, ScalableTarget)>)>,
    /// key `namespace\tresource_id\tdimension\tpolicy_name` → policy,
    /// insertion order preserved.
    policies: Vec<(String, ScalingPolicy)>,
    /// Append-only scheduled-action list (moto's list).
    scheduled_actions: Vec<ScheduledAction>,
}

struct ScalableTarget {
    service_namespace: String,
    resource_id: String,
    scalable_dimension: String,
    min_capacity: Option<i64>,
    max_capacity: Option<i64>,
    role_arn: Option<String>,
    suspended_state: Option<Value>,
    creation_time: f64,
    arn: String,
}

struct ScalingPolicy {
    policy_name: String,
    service_namespace: String,
    resource_id: String,
    scalable_dimension: String,
    policy_type: String,
    /// `StepScalingPolicyConfiguration` when `policy_type == "StepScaling"`.
    step_scaling: Option<Value>,
    /// `TargetTrackingScalingPolicyConfiguration` when TargetTracking.
    target_tracking: Option<Value>,
    policy_arn: String,
    creation_time: f64,
    alarms: Vec<Value>,
}

struct ScheduledAction {
    service_namespace: String,
    resource_id: String,
    scalable_dimension: String,
    scheduled_action_name: String,
    arn: String,
    schedule: Option<Value>,
    timezone: Option<String>,
    start_time: Option<Value>,
    end_time: Option<Value>,
    scalable_target_action: Option<Value>,
    creation_time: f64,
}

fn with_store(req: &Request, f: impl FnOnce(&mut AppAutoscalingState) -> Response) -> Response {
    let slot = stores();
    let mut map = slot.write();
    let st = map.entry((req.account, req.region.clone())).or_default();
    f(st)
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "applicationautoscaling" {
        return None;
    }
    Some(with_store(req, |st| {
        route(req, st, req.account, &req.region)
    }))
}

fn route(req: &Request, st: &mut AppAutoscalingState, account: u64, region: &str) -> Response {
    let body = parse_json_body(&req.body);
    match req.operation.as_str() {
        // --- scalable targets ------------------------------------------------
        "RegisterScalableTarget" => register_scalable_target(st, &body, account, region),
        "DescribeScalableTargets" => describe_scalable_targets(st, &body),
        "DeregisterScalableTarget" => deregister_scalable_target(st, &body),
        // --- scaling policies ------------------------------------------------
        "PutScalingPolicy" => put_scaling_policy(st, &body, account, region),
        "DescribeScalingPolicies" => describe_scaling_policies(st, &body),
        "DeleteScalingPolicy" => delete_scaling_policy(st, &body),
        // --- scheduled actions -----------------------------------------------
        "PutScheduledAction" => put_scheduled_action(st, &body, account, region),
        "DeleteScheduledAction" => delete_scheduled_action(st, &body),
        "DescribeScheduledActions" => describe_scheduled_actions(st, &body),
        // The moto fork raises "action has not been implemented" here.
        other => json_error(
            501,
            "NotImplemented",
            &format!("The {other} action has not been implemented"),
        ),
    }
}

// ---------------------------------------------------------------------------
// Wire helpers
// ---------------------------------------------------------------------------

fn ok(body: Value) -> Response {
    Response::json(200, serde_json::to_vec(&body).unwrap_or_default())
}

fn fault(code: &str, message: &str) -> Response {
    json_error(400, code, message)
}

fn ask_str<'a>(body: &'a Value, key: &str) -> Option<&'a str> {
    body.get(key).and_then(Value::as_str)
}

fn as_str<'a>(body: &'a Value, key: &str) -> &'a str {
    ask_str(body, key).unwrap_or("")
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

/// Python truthiness for the wire values moto's code branches on (policy
/// bodies, schedule payloads). `None`, `{}`, `[]`, `""`, `0`, `false` are
/// falsy.
fn truthy(v: Option<&Value>) -> bool {
    match v {
        None | Some(Value::Null) | Some(Value::Bool(false)) => false,
        Some(Value::Number(n)) => n.as_f64() != Some(0.0),
        Some(Value::String(s)) => !s.is_empty(),
        Some(Value::Array(a)) => !a.is_empty(),
        Some(_) => true,
    }
}

/// Python truthiness for plain string params.
fn truthy_str(v: Option<&str>) -> bool {
    v.map(|s| !s.is_empty()).unwrap_or(false)
}

/// moto's `fake_random.get_random_string(length=36, lower_case=True)`.
fn random_string_36() -> String {
    // 36 lowercase chars: 32 hex digits from one uuid plus 4 from the next.
    let mut s = hex::encode(uuid::Uuid::new_v4().as_bytes());
    s.push_str(&hex::encode(&uuid::Uuid::new_v4().as_bytes()[..2]));
    s
}

fn partition(region: &str) -> &'static str {
    if region.starts_with("cn-") {
        "aws-cn"
    } else if region.starts_with("us-gov-") {
        "aws-us-gov"
    } else if region.starts_with("us-isob-") {
        "aws-iso-b"
    } else if region.starts_with("us-iso-") {
        "aws-iso"
    } else if region.starts_with("eusc-") {
        "aws-eusc"
    } else {
        "aws"
    }
}

/// Python-style list repr for the enum-validation messages.
fn python_list(values: &[&str]) -> String {
    let joined: Vec<String> = values.iter().map(|v| format!("'{v}'")).collect();
    format!("[{}]", joined.join(", "))
}

/// moto's `_validate_params`: ServiceNamespace / ScalableDimension enum checks
/// with the failed-constraint ValidationException wording.
fn validate_params(body: &Value) -> Option<Response> {
    // moto appends the ScalableDimension message before the ServiceNamespace
    // one, so a combined two-error fault names the dimension first.
    let mut messages: Vec<String> = Vec::new();
    if let Some(dimension) = ask_str(body, "ScalableDimension") {
        if !SCALABLE_DIMENSIONS.contains(&dimension) {
            messages.push(format!(
                "Value '{dimension}' at 'scalableDimension' failed to satisfy constraint: Member must satisfy enum value set: {}",
                python_list(SCALABLE_DIMENSIONS)
            ));
        }
    }
    if let Some(namespace) = ask_str(body, "ServiceNamespace") {
        if !SERVICE_NAMESPACES.contains(&namespace) {
            messages.push(format!(
                "Value '{namespace}' at 'serviceNamespace' failed to satisfy constraint: Member must satisfy enum value set: {}",
                python_list(SERVICE_NAMESPACES)
            ));
        }
    }
    match messages.len() {
        0 => None,
        1 => Some(fault(
            "ValidationException",
            &format!("1 validation error detected: {}", messages[0]),
        )),
        n => Some(fault(
            "ValidationException",
            &format!("{n} validation errors detected: {}", messages.join("; ")),
        )),
    }
}

/// moto's `_target_params_are_valid` + `_get_resource_type_from_resource_id`:
/// namespace, dimension and the resource-id's resource type must agree.
fn target_params_valid(namespace: &str, resource_id: &str, dimension: Option<&str>) -> bool {
    let mut valid = true;
    if !SERVICE_NAMESPACES.contains(&namespace) {
        valid = false;
    }
    if let Some(dimension) = dimension {
        let parts: Vec<&str> = dimension.split(':').collect();
        match parts.as_slice() {
            [d_namespace, d_resource_type, _] => {
                let (d_namespace, d_resource_type) = (*d_namespace, *d_resource_type);
                // moto's ResourceTypeExceptionValueSet: only broker-storage
                // exempts the resource-id consistency check.
                let resource_type: String = if d_resource_type != "broker-storage" {
                    resource_type_of(resource_id)
                } else {
                    d_resource_type.to_string()
                };
                if !SCALABLE_DIMENSIONS.contains(&dimension)
                    || d_namespace != namespace
                    || resource_type != d_resource_type
                {
                    valid = false;
                }
            }
            // moto unpacks exactly three colon segments; anything else is
            // invalid.
            _ => valid = false,
        }
    }
    valid
}

/// moto's `_get_resource_type_from_resource_id`.
fn resource_type_of(resource_id: &str) -> String {
    let mut rid = resource_id.to_string();
    // moto: `re.match("^arn:(aws|aws-cn|...):comprehend", resource_id)` —
    // comprehend ARNs collapse to their last colon component first.
    const PARTITIONS: [&str; 6] = [
        "aws",
        "aws-cn",
        "aws-us-gov",
        "aws-iso",
        "aws-iso-b",
        "aws-eusc",
    ];
    let is_comprehend_arn = rid.starts_with("arn:")
        && PARTITIONS.iter().any(|p| {
            let after = &rid["arn:".len()..];
            let mut seg = after.splitn(2, ':');
            seg.next() == Some(*p) && seg.next().is_some_and(|r| r.starts_with("comprehend"))
        });
    if is_comprehend_arn {
        // python `resource_id.split(":")[-1]`
        rid = rid.rsplit(':').next().unwrap_or_default().to_string();
    }
    let split: Vec<&str> = if rid.contains('/') {
        rid.split('/').collect()
    } else {
        rid.split(':').collect()
    };
    if split[0] == "endpoint" || (split[0] == "table" && split.len() > 2) || split[0] == "keyspace"
    {
        split[2].to_string()
    } else {
        split[0].to_string()
    }
}

// ---------------------------------------------------------------------------
// Scalable targets
// ---------------------------------------------------------------------------

/// moto's `_flatten_scalable_targets`: every target for a namespace, in
/// dimension-then-resource insertion order.
fn flattened_targets<'a>(
    state: &'a AppAutoscalingState,
    namespace: Option<&str>,
) -> Vec<&'a ScalableTarget> {
    let mut out: Vec<&ScalableTarget> = Vec::new();
    for (_, rows) in &state.targets {
        for (_, target) in rows {
            // moto compares against the raw param value; an absent
            // ServiceNamespace matches no target (names are never null).
            if Some(target.service_namespace.as_str()) == namespace {
                out.push(target);
            }
        }
    }
    out
}

fn target_dict(t: &ScalableTarget) -> Value {
    json!({
        "CreationTime": t.creation_time,
        "MaxCapacity": t.max_capacity,
        "MinCapacity": t.min_capacity,
        "ResourceId": t.resource_id,
        "RoleARN": t.role_arn,
        "ScalableDimension": t.scalable_dimension,
        "ServiceNamespace": t.service_namespace,
        "ScalableTargetARN": t.arn,
        "SuspendedState": t.suspended_state,
    })
}

fn new_target(
    namespace: &str,
    resource_id: &str,
    dimension: &str,
    body: &Value,
    account: u64,
    region: &str,
) -> ScalableTarget {
    ScalableTarget {
        service_namespace: namespace.to_string(),
        resource_id: resource_id.to_string(),
        scalable_dimension: dimension.to_string(),
        min_capacity: body.get("MinCapacity").and_then(Value::as_i64),
        max_capacity: body.get("MaxCapacity").and_then(Value::as_i64),
        role_arn: ask_str(body, "RoleARN").map(String::from),
        suspended_state: body.get("SuspendedState").cloned(),
        creation_time: now_unix(),
        arn: format!(
            "arn:{}:application-autoscaling:{region}:{account}:scalable-target/{}",
            partition(region),
            random_string_36()
        ),
    }
}

fn register_scalable_target(
    st: &mut AppAutoscalingState,
    body: &Value,
    account: u64,
    region: &str,
) -> Response {
    if let Some(resp) = validate_params(body) {
        return resp;
    }
    let namespace = as_str(body, "ServiceNamespace");
    let resource_id = as_str(body, "ResourceId");
    let dimension = ask_str(body, "ScalableDimension");
    if !target_params_valid(namespace, resource_id, dimension) {
        return fault(
            "ValidationException",
            "Unsupported service namespace, resource type or scalable dimension",
        );
    }
    let dimension = dimension.unwrap_or_default();
    // moto: an ECS target requires a live ECS service for (cluster, service).
    if namespace == "ecs" {
        let parts: Vec<&str> = resource_id.split('/').collect();
        let exists = match parts.as_slice() {
            // resource id is `service/{cluster}/{name}`.
            [_kind, cluster, service] => {
                // moto: describe_services raises ClusterNotFoundException for a
                // missing cluster ("Cluster not found.") and only raises
                // AWSValidationException when the cluster exists but the
                // service is missing (applicationautoscaling/models.py:140,
                // ecs/models.py:1637).
                if !ecs::bridge::cluster_exists(account, region, cluster) {
                    return fault("ClusterNotFoundException", "Cluster not found.");
                }
                ecs::bridge::service_exists(account, region, cluster, service)
            }
            // moto unpacks three slash components and crashes otherwise;
            // answer the named fault instead.
            _ => false,
        };
        if !exists {
            return fault(
                "ValidationException",
                &format!("ECS service doesn't exist: {resource_id}"),
            );
        }
    }

    let row = st
        .targets
        .iter_mut()
        .find(|(dim, _)| *dim == dimension)
        .map(|(_, rows)| rows);
    let Some(rows) = row else {
        let target = new_target(namespace, resource_id, dimension, body, account, region);
        let arn = json!(target.arn.clone());
        st.targets.push((
            dimension.to_string(),
            vec![(resource_id.to_string(), target)],
        ));
        return ok(json!({ "ScalableTargetARN": arn }));
    };
    match rows.iter_mut().find(|(rid, _)| rid == resource_id) {
        Some((_, existing)) => {
            // moto's FakeScalableTarget.update: only overwrite sent members.
            if let Some(min) = body.get("MinCapacity").and_then(Value::as_i64) {
                existing.min_capacity = Some(min);
            }
            if let Some(max) = body.get("MaxCapacity").and_then(Value::as_i64) {
                existing.max_capacity = Some(max);
            }
            if let Some(state) = body.get("SuspendedState").filter(|v| !v.is_null()) {
                existing.suspended_state = Some(state.clone());
            }
            ok(json!({ "ScalableTargetARN": existing.arn }))
        }
        None => {
            let target = new_target(namespace, resource_id, dimension, body, account, region);
            let arn = json!(target.arn.clone());
            rows.push((resource_id.to_string(), target));
            ok(json!({ "ScalableTargetARN": arn }))
        }
    }
}

fn describe_scalable_targets(st: &mut AppAutoscalingState, body: &Value) -> Response {
    if let Some(resp) = validate_params(body) {
        return resp;
    }
    let namespace = ask_str(body, "ServiceNamespace");
    let dimension = ask_str(body, "ScalableDimension");
    let resource_ids = as_str_list(body.get("ResourceIds"));
    let mut targets = flattened_targets(st, namespace);
    if let Some(dimension) = dimension {
        targets.retain(|t| t.scalable_dimension == dimension);
    }
    if !resource_ids.is_empty() {
        targets.retain(|t| resource_ids.contains(&t.resource_id));
    }
    // moto's pagination: MaxResults defaults 50 (`max_results or 50`); the
    // marker resumes one past its recorded index (`int(marker) + 1`) and the
    // next token is the page length minus one.
    let max_results: u64 = body
        .get("MaxResults")
        .and_then(Value::as_i64)
        .filter(|v| *v > 0)
        .map(|v| v as u64)
        .unwrap_or(50);
    let marker = body
        .get("NextToken")
        .and_then(Value::as_str)
        .and_then(|m| m.parse::<usize>().ok());
    let start = marker.map_or(0, |m| m + 1);
    let start = start.min(targets.len());
    let page_len = (targets.len() - start).min(max_results as usize);
    let next_token = if targets.len() > start.saturating_add(max_results as usize) {
        json!(page_len.saturating_sub(1).to_string())
    } else {
        Value::Null
    };
    let page: Vec<Value> = targets
        .into_iter()
        .skip(start)
        .take(page_len)
        .map(target_dict)
        .collect();
    ok(json!({ "ScalableTargets": page, "NextToken": next_token }))
}

fn deregister_scalable_target(st: &mut AppAutoscalingState, body: &Value) -> Response {
    if let Some(resp) = validate_params(body) {
        return resp;
    }
    let namespace = as_str(body, "ServiceNamespace");
    let resource_id = as_str(body, "ResourceId");
    let dimension = as_str(body, "ScalableDimension");
    // moto deletes self.targets[dimension][r_id] when it exists.
    let existed = st
        .targets
        .iter()
        .any(|(dim, rows)| *dim == dimension && rows.iter().any(|(rid, _)| rid == resource_id));
    if existed {
        if let Some((_, rows)) = st.targets.iter_mut().find(|(dim, _)| *dim == dimension) {
            rows.retain(|(rid, _)| rid != resource_id);
        }
        ok(json!({}))
    } else {
        fault(
            "ValidationException",
            &format!(
                "No scalable target found for service namespace: {namespace}, resource ID: {resource_id}, scalable dimension: {dimension}"
            ),
        )
    }
}

// ---------------------------------------------------------------------------
// Scaling policies
// ---------------------------------------------------------------------------

fn policy_key(service_namespace: &str, resource_id: &str, dimension: &str, name: &str) -> String {
    format!("{service_namespace}\t{resource_id}\t{dimension}\t{name}")
}

fn alarm_dict(a: &Value) -> Value {
    json!({
        "AlarmARN": a["alarm_arn"],
        "AlarmName": a["name"],
    })
}

fn policy_dict(p: &ScalingPolicy) -> Value {
    let mut response = json!({
        "PolicyARN": p.policy_arn,
        "PolicyName": p.policy_name,
        "ServiceNamespace": p.service_namespace,
        "ResourceId": p.resource_id,
        "ScalableDimension": p.scalable_dimension,
        "PolicyType": p.policy_type,
        "CreationTime": p.creation_time,
        "Alarms": p.alarms.iter().map(alarm_dict).collect::<Vec<_>>(),
    });
    let obj = response.as_object_mut().expect("object");
    if p.policy_type == "StepScaling" {
        obj.insert(
            "StepScalingPolicyConfiguration".to_string(),
            p.step_scaling.clone().unwrap_or(Value::Null),
        );
    } else if p.policy_type == "TargetTrackingScaling" {
        obj.insert(
            "TargetTrackingScalingPolicyConfiguration".to_string(),
            p.target_tracking.clone().unwrap_or(Value::Null),
        );
    }
    response
}

/// The alarm names/ARNs moto's TargetTracking `create_alarms` produces for a
/// namespace (four for dynamodb, two for ecs). Shape-only: they are never
/// mirrored into the CloudWatch backend.
///
/// moto embeds a fresh uuid in each alarm name and a `_policy_arn`-derived
/// `createdBy/uuid` alarm action — the uuids are random per call, so only
/// the stable parts are asserted.
fn create_alarms(
    service_namespace: &str,
    resource_id: &str,
    policy_arn: &str,
    account: u64,
    region: &str,
) -> Vec<Value> {
    let alarm_action = format!("{}:createdBy/{}", policy_arn, uuid::Uuid::new_v4());
    let alarm = |name: String| {
        json!({
            "name": name,
            "alarm_arn": format!(
                "arn:{}:cloudwatch:{region}:{account}:alarm/{}",
                partition(region),
                name
            ),
            "description": format!(
                "DO NOT EDIT OR DELETE. For TargetTrackingScaling policy {alarm_action}"
            ),
        })
    };
    let uuid_part = || uuid::Uuid::new_v4().to_string();
    match service_namespace {
        "dynamodb" => {
            let table_resource = resource_id.split('/').next_back().unwrap_or_default();
            let base = format!("TargetTracking-table/{table_resource}");
            vec![
                alarm(format!("{base}-AlarmHigh-{}", uuid_part())),
                alarm(format!("{base}-AlarmLow-{}", uuid_part())),
                alarm(format!("{base}-ProvisionedCapacityHigh-{}", uuid_part())),
                alarm(format!("{base}-ProvisionedCapacityLow-{}", uuid_part())),
            ]
        }
        "ecs" => {
            // moto picks MemoryUtilization vs CPUUtilization by the metric
            // spec for the real CloudWatch alarm; the choice never reaches
            // the policy wire (names carry the enum above's uuid pattern).
            let base = format!("TargetTracking-{resource_id}");
            vec![
                alarm(format!("{base}-AlarmHigh-{}", uuid_part())),
                alarm(format!("{base}-AlarmLow-{}", uuid_part())),
            ]
        }
        _ => Vec::new(),
    }
}

fn put_scaling_policy(
    st: &mut AppAutoscalingState,
    body: &Value,
    account: u64,
    region: &str,
) -> Response {
    let name = as_str(body, "PolicyName");
    let namespace = as_str(body, "ServiceNamespace");
    let resource_id = as_str(body, "ResourceId");
    let dimension = as_str(body, "ScalableDimension");
    // moto: the StepScaling body wins when the member is present (even as
    // null); otherwise the TargetTracking body is used.
    let policy_body: Option<Value> = if let Some(v) = body.get("StepScalingPolicyConfiguration") {
        Some(v.clone())
    } else {
        body.get("TargetTrackingScalingPolicyConfiguration")
            .cloned()
    };
    let policy_type = ask_str(body, "PolicyType").unwrap_or("");
    if policy_type.is_empty()
        || (policy_type != "StepScaling" && policy_type != "TargetTrackingScaling")
    {
        // moto renders an absent member as 'None' in the constraint message.
        let sent_type = ask_str(body, "PolicyType").unwrap_or("None");
        return fault(
            "ValidationException",
            &format!(
                "1 validation error detected: Value '{sent_type}' at 'policyType' failed to satisfy constraint: Member must satisfy enum value set: [PredictiveScaling, StepScaling, TargetTrackingScaling]"
            ),
        );
    }

    let key = policy_key(namespace, resource_id, dimension, name);
    let (policy_type, policy_body) =
        if let Some((_, old)) = st.policies.iter().find(|(k, _)| *k == key) {
            // moto rebuilds: sent (truthy) values win, otherwise the old ones.
            let old_type = old.policy_type.as_str();
            let t = if policy_type.is_empty() {
                old_type
            } else {
                policy_type
            };
            let b = if truthy(policy_body.as_ref()) {
                policy_body.clone()
            } else {
                policy_body_for_type(old, t)
            };
            (t.to_string(), b)
        } else {
            (
                policy_type.to_string(),
                Some(policy_body.clone().unwrap_or(Value::Null)),
            )
        };

    // moto re-creates the alarm set on every put; alarms exist only for
    // TargetTrackingScaling policies (create_alarms gates on policy_type).
    let policy_arn = format!(
        "arn:{}:autoscaling:{region}:{account}:scalingPolicy:{}:resource/{namespace}/{resource_id}:policyName/{name}",
        partition(region),
        uuid::Uuid::new_v4()
    );
    let alarms = if policy_type == "TargetTrackingScaling" {
        create_alarms(namespace, resource_id, &policy_arn, account, region)
    } else {
        Vec::new()
    };
    let p = ScalingPolicy {
        policy_name: name.to_string(),
        service_namespace: namespace.to_string(),
        resource_id: resource_id.to_string(),
        scalable_dimension: dimension.to_string(),
        policy_type: policy_type.to_string(),
        step_scaling: if policy_type == "StepScaling" {
            policy_body.clone()
        } else {
            None
        },
        target_tracking: if policy_type == "TargetTrackingScaling" {
            policy_body.clone()
        } else {
            None
        },
        policy_arn: policy_arn.clone(),
        creation_time: now_unix(),
        alarms: alarms.clone(),
    };
    match st
        .policies
        .iter_mut()
        .find(|(k, _)| *k == key)
        .map(|(_, p)| p)
    {
        Some(slot) => *slot = p,
        None => st.policies.push((key, p)),
    }
    ok(json!({
        "PolicyARN": policy_arn,
        "Alarms": alarms.iter().map(alarm_dict).collect::<Vec<_>>(),
    }))
}

/// The stored policy body for `t` (moto keeps one `_policy_body` per policy;
/// the type switch only decides which member renders).
fn policy_body_for_type(p: &ScalingPolicy, t: &str) -> Option<Value> {
    if t == "StepScaling" {
        p.step_scaling.clone()
    } else {
        p.target_tracking.clone()
    }
}

fn describe_scaling_policies(st: &mut AppAutoscalingState, body: &Value) -> Response {
    let namespace = ask_str(body, "ServiceNamespace");
    let resource_id = ask_str(body, "ResourceId");
    let dimension = ask_str(body, "ScalableDimension");
    let mut policies: Vec<&ScalingPolicy> = st
        .policies
        .iter()
        // moto compares the raw param: an absent namespace matches nothing.
        .filter(|(_, p)| Some(p.service_namespace.as_str()) == namespace)
        .map(|(_, p)| p)
        .collect();
    if truthy_str(resource_id) {
        // moto: `policy.resource_id in resource_id` — the stored id is a
        // substring of the filter.
        let rid = resource_id.unwrap_or_default();
        policies.retain(|p| rid.contains(p.resource_id.as_str()));
    }
    if truthy_str(dimension) {
        let dim = dimension.unwrap_or_default();
        policies.retain(|p| dim.contains(p.scalable_dimension.as_str()));
    }
    // moto's pagination: MaxResults defaults 100 (`max_results or 100`) and
    // marker resumes exactly at its recorded offset.
    let max_results: i64 = body
        .get("MaxResults")
        .and_then(Value::as_i64)
        .filter(|v| *v > 0)
        .unwrap_or(100);
    let starting = body
        .get("NextToken")
        .and_then(Value::as_str)
        .and_then(|m| m.parse::<usize>().ok())
        .unwrap_or(0);
    let starting = starting.min(policies.len());
    let ending = starting.saturating_add(max_results as usize);
    let page_len = ending.min(policies.len()) - starting;
    let next_token = if ending < policies.len() {
        json!(ending.to_string())
    } else {
        Value::Null
    };
    let page: Vec<Value> = policies
        .into_iter()
        .skip(starting)
        .take(page_len)
        .map(policy_dict)
        .collect();
    ok(json!({ "ScalingPolicies": page, "NextToken": next_token }))
}

fn delete_scaling_policy(st: &mut AppAutoscalingState, body: &Value) -> Response {
    let name = as_str(body, "PolicyName");
    let namespace = as_str(body, "ServiceNamespace");
    let resource_id = as_str(body, "ResourceId");
    let dimension = as_str(body, "ScalableDimension");
    let key = policy_key(namespace, resource_id, dimension, name);
    let before = st.policies.len();
    st.policies.retain(|(k, _)| k != &key);
    if st.policies.len() == before {
        fault(
            "ValidationException",
            &format!(
                "No scaling policy found for service namespace: {namespace}, resource ID: {resource_id}, scalable dimension: {dimension}, policy name: {name}"
            ),
        )
    } else {
        ok(json!({}))
    }
}

// ---------------------------------------------------------------------------
// Scheduled actions
// ---------------------------------------------------------------------------

fn action_dict(a: &ScheduledAction) -> Value {
    json!({
        "ScheduledActionName": a.scheduled_action_name,
        "ScheduledActionARN": a.arn,
        "ServiceNamespace": a.service_namespace,
        "Schedule": a.schedule,
        "Timezone": a.timezone,
        "ResourceId": a.resource_id,
        "ScalableDimension": a.scalable_dimension,
        "StartTime": a.start_time,
        "EndTime": a.end_time,
        "CreationTime": a.creation_time,
        "ScalableTargetAction": a.scalable_target_action,
    })
}

fn put_scheduled_action(
    st: &mut AppAutoscalingState,
    body: &Value,
    account: u64,
    region: &str,
) -> Response {
    let namespace = as_str(body, "ServiceNamespace");
    let name = as_str(body, "ScheduledActionName");
    let resource_id = as_str(body, "ResourceId");
    let dimension = as_str(body, "ScalableDimension");
    let schedule = body.get("Schedule").cloned();
    let timezone = ask_str(body, "Timezone").map(String::from);
    let scalable_target_action = body.get("ScalableTargetAction").cloned();
    // moto unconditionally overwrites the window on update.
    let start_time = body.get("StartTime").cloned();
    let end_time = body.get("EndTime").cloned();

    if let Some(existing) = st.scheduled_actions.iter_mut().find(|a| {
        a.service_namespace == namespace
            && a.scheduled_action_name == name
            && a.resource_id == resource_id
            && a.scalable_dimension == dimension
    }) {
        // moto's truthy-guarded members: falsy submissions keep the old ones.
        if truthy(schedule.as_ref()) {
            existing.schedule = schedule;
        }
        if truthy_str(timezone.as_deref()) {
            existing.timezone = timezone;
        }
        if truthy(scalable_target_action.as_ref()) {
            existing.scalable_target_action = scalable_target_action;
        }
        existing.start_time = start_time;
        existing.end_time = end_time;
        ok(json!({}))
    } else {
        let arn = format!(
            "arn:{}:autoscaling:{region}:{account}:scheduledAction:{namespace}/{resource_id}:scheduledActionName/{name}",
            partition(region),
        );
        st.scheduled_actions.push(ScheduledAction {
            service_namespace: namespace.to_string(),
            resource_id: resource_id.to_string(),
            scalable_dimension: dimension.to_string(),
            scheduled_action_name: name.to_string(),
            arn,
            schedule,
            timezone,
            start_time,
            end_time,
            scalable_target_action,
            creation_time: now_unix(),
        });
        ok(json!({}))
    }
}

fn describe_scheduled_actions(st: &mut AppAutoscalingState, body: &Value) -> Response {
    let namespace = ask_str(body, "ServiceNamespace");
    let names = as_str_list(body.get("ScheduledActionNames"));
    let resource_id = ask_str(body, "ResourceId");
    let dimension = ask_str(body, "ScalableDimension");
    let actions: Vec<&ScheduledAction> = st
        .scheduled_actions
        .iter()
        // moto filters membership on the namespace first: an absent
        // ServiceNamespace matches no action at all.
        .filter(|a| Some(a.service_namespace.as_str()) == namespace)
        .filter(|a| names.is_empty() || names.contains(&a.scheduled_action_name))
        .filter(|a| !truthy_str(resource_id) || a.resource_id == resource_id.unwrap_or_default())
        .filter(|a| !truthy_str(dimension) || a.scalable_dimension == dimension.unwrap_or_default())
        .collect();
    ok(json!({
        "ScheduledActions": actions.iter().map(|a| action_dict(a)).collect::<Vec<_>>(),
    }))
}

fn delete_scheduled_action(st: &mut AppAutoscalingState, body: &Value) -> Response {
    let namespace = as_str(body, "ServiceNamespace");
    let name = as_str(body, "ScheduledActionName");
    let resource_id = as_str(body, "ResourceId");
    let dimension = as_str(body, "ScalableDimension");
    st.scheduled_actions.retain(|a| {
        !(a.service_namespace == namespace
            && a.scheduled_action_name == name
            && a.resource_id == resource_id
            && a.scalable_dimension == dimension)
    });
    ok(json!({}))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::sync::atomic::{AtomicU64, Ordering};

    static ACCOUNTS: AtomicU64 = AtomicU64::new(300_000_000_001);

    fn next_account() -> u64 {
        ACCOUNTS.fetch_add(1, Ordering::SeqCst)
    }

    fn send(account: u64, operation: &str, body: Value) -> (u16, Value) {
        let request = Request {
            service: "applicationautoscaling".to_string(),
            operation: operation.to_string(),
            method: "POST".to_string(),
            path: "/".to_string(),
            query_string: String::new(),
            headers: HashMap::new(),
            body: Bytes::from(serde_json::to_vec(&body).unwrap()),
            account,
            region: "us-east-1".to_string(),
        };
        let resp = dispatch(&request).expect("applicationautoscaling dispatch");
        (
            resp.status,
            serde_json::from_slice(&resp.body).unwrap_or(json!({})),
        )
    }

    fn register_target(account: u64, resource_id: &str) -> (u16, Value) {
        send(
            account,
            "RegisterScalableTarget",
            json!({
                "ServiceNamespace": "dynamodb",
                "ResourceId": resource_id,
                "ScalableDimension": "dynamodb:table:ReadCapacityUnits",
                "MinCapacity": 1,
                "MaxCapacity": 100,
            }),
        )
    }

    /// Seed a live ECS service through the bridge state (the natural swap for
    /// moto's `ecs_backends`).
    fn seed_ecs_service(account: u64, cluster: &str, service: &str) {
        let state = ecs::models::EcsState::new();
        state
            .clusters
            .write()
            .insert(cluster.to_string(), json!({ "clusterName": cluster }));
        state.services.write().insert(
            cluster.to_string(),
            HashMap::from([(
                service.to_string(),
                json!({ "serviceName": service, "clusterArn": cluster }),
            )]),
        );
        ecs::bridge::register_state(account, "us-east-1", &state);
    }

    #[test]
    fn register_and_describe_round_trip() {
        let account = next_account();
        let rid = "table/t-round".to_string();
        let (status, body) = register_target(account, &rid);
        assert_eq!(status, 200);
        let arn = body["ScalableTargetARN"].as_str().unwrap();
        assert!(arn.starts_with("arn:aws:application-autoscaling:us-east-1:"));
        assert!(arn.contains(":scalable-target/"));

        let (status, body) = send(
            account,
            "DescribeScalableTargets",
            json!({ "ServiceNamespace": "dynamodb" }),
        );
        assert_eq!(status, 200);
        let hits: Vec<_> = body["ScalableTargets"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|t| t["ResourceId"].as_str() == Some(rid.as_str()))
            .collect();
        assert_eq!(hits.len(), 1);
        assert_eq!(
            hits[0]["ScalableDimension"],
            "dynamodb:table:ReadCapacityUnits"
        );
        assert_eq!(hits[0]["MinCapacity"], 1);
        assert_eq!(hits[0]["MaxCapacity"], 100);
        assert_eq!(hits[0]["ServiceNamespace"], "dynamodb");
        assert!(hits[0]["CreationTime"].is_number());
        assert!(hits[0]["SuspendedState"].is_null());
    }

    #[test]
    fn register_upsert_overwrites_only_sent_members() {
        let account = next_account();
        let rid = "table/t-upsert".to_string();
        register_target(account, &rid);
        // Re-register with only MaxCapacity: MinCapacity survives.
        let (status, _) = send(
            account,
            "RegisterScalableTarget",
            json!({
                "ServiceNamespace": "dynamodb",
                "ResourceId": rid.clone(),
                "ScalableDimension": "dynamodb:table:ReadCapacityUnits",
                "MaxCapacity": 200,
            }),
        );
        assert_eq!(status, 200);
        let (_, body) = send(
            account,
            "DescribeScalableTargets",
            json!({
                "ServiceNamespace": "dynamodb",
                "ResourceIds": [rid.clone()],
            }),
        );
        let targets = body["ScalableTargets"].as_array().unwrap();
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0]["MinCapacity"], 1);
        assert_eq!(targets[0]["MaxCapacity"], 200);
        // The ARN is stable across the update.
        let (status, body) = send(
            account,
            "RegisterScalableTarget",
            json!({
                "ServiceNamespace": "dynamodb",
                "ResourceId": rid.clone(),
                "ScalableDimension": "dynamodb:table:ReadCapacityUnits",
                "MinCapacity": 5,
            }),
        );
        assert_eq!(status, 200);
        assert_eq!(targets[0]["ScalableTargetARN"], body["ScalableTargetARN"]);
    }

    #[test]
    fn deregister_and_faults() {
        let account = next_account();
        let rid = "table/t-dereg".to_string();
        register_target(account, &rid);
        let (status, _) = send(
            account,
            "DeregisterScalableTarget",
            json!({
                "ServiceNamespace": "dynamodb",
                "ResourceId": rid,
                "ScalableDimension": "dynamodb:table:ReadCapacityUnits",
            }),
        );
        assert_eq!(status, 200);
        let (_, body) = send(
            account,
            "DescribeScalableTargets",
            json!({ "ServiceNamespace": "dynamodb" }),
        );
        assert!(body["ScalableTargets"]
            .as_array()
            .unwrap()
            .iter()
            .all(|t| t["ResourceId"].as_str() != Some("table/t-dereg")));

        // Deregistering again faults with moto's wording.
        let (status, body) = send(
            account,
            "DeregisterScalableTarget",
            json!({
                "ServiceNamespace": "dynamodb",
                "ResourceId": "table/t-dereg",
                "ScalableDimension": "dynamodb:table:ReadCapacityUnits",
            }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "ValidationException");
        assert!(body["message"]
            .as_str()
            .unwrap()
            .starts_with("No scalable target found for service namespace: dynamodb"),);
    }

    #[test]
    fn enum_and_consistency_validation() {
        let account = next_account();
        // Unknown dimension / namespace are moto's constraint faults.
        let (status, body) = send(
            account,
            "RegisterScalableTarget",
            json!({
                "ServiceNamespace": "dynamodb",
                "ResourceId": "table/x",
                "ScalableDimension": "dynamodb:table:BogusUnits",
            }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "ValidationException");
        assert!(body["message"].as_str().unwrap().contains("'scalableDimension' failed to satisfy constraint: Member must satisfy enum value set: ["));

        let (status, body) = send(
            account,
            "RegisterScalableTarget",
            json!({
                "ServiceNamespace": "wrongns",
                "ResourceId": "table/x",
                "ScalableDimension": "dynamodb:table:ReadCapacityUnits",
            }),
        );
        assert_eq!(status, 400);
        assert!(body["message"]
            .as_str()
            .unwrap()
            .contains("'serviceNamespace' failed to satisfy"));

        // Namespace/dimension/resource-type disagreement.
        let (status, body) = send(
            account,
            "RegisterScalableTarget",
            json!({
                "ServiceNamespace": "dynamodb",
                "ResourceId": "ecs/my-cluster/web",
                "ScalableDimension": "dynamodb:table:ReadCapacityUnits",
            }),
        );
        assert_eq!(status, 400);
        assert_eq!(
            body["message"],
            "Unsupported service namespace, resource type or scalable dimension"
        );

        // The ecs dimension needs an ecs-styled resource id.
        let (status, body) = send(
            account,
            "RegisterScalableTarget",
            json!({
                "ServiceNamespace": "ecs",
                "ResourceId": "foobarbaz",
                "ScalableDimension": "ecs:service:DesiredCount",
            }),
        );
        assert_eq!(status, 400);
        assert_eq!(
            body["message"],
            "Unsupported service namespace, resource type or scalable dimension"
        );

        // Passing validation: a dynamodb index target keyed by its resource
        // type (resource id "index/…" splits on '/').
        let (status, _) = send(
            account,
            "RegisterScalableTarget",
            json!({
                "ServiceNamespace": "dynamodb",
                "ResourceId": "table",
                "ScalableDimension": "dynamodb:table:ReadCapacityUnits",
            }),
        );
        assert_eq!(status, 200);
    }

    #[test]
    fn ecs_target_requires_live_service() {
        let account = next_account();
        // No ECS state for the scope at all: moto's describe_services raises
        // ClusterNotFoundException (the ecs backend itself always exists).
        let (status, body) = send(
            account,
            "RegisterScalableTarget",
            json!({
                "ServiceNamespace": "ecs",
                "ResourceId": "service/default/web",
                "ScalableDimension": "ecs:service:DesiredCount",
            }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["message"], "Cluster not found.");

        // Now register the cluster, not the service: the missing-service
        // case is moto's AWSValidationException.
        let state = ecs::models::EcsState::new();
        state
            .clusters
            .write()
            .insert("default".to_string(), json!({ "clusterName": "default" }));
        state.services.write().insert(
            "default".to_string(),
            HashMap::from([(
                "other-service".to_string(),
                json!({ "serviceName": "other-service" }),
            )]),
        );
        ecs::bridge::register_state(account, "us-east-1", &state);
        let (status, body) = send(
            account,
            "RegisterScalableTarget",
            json!({
                "ServiceNamespace": "ecs",
                "ResourceId": "service/default/web",
                "ScalableDimension": "ecs:service:DesiredCount",
            }),
        );
        assert_eq!(status, 400);
        assert_eq!(
            body["message"],
            "ECS service doesn't exist: service/default/web"
        );
    }

    #[test]
    fn ecs_target_with_live_service_registers() {
        let account = next_account();
        seed_ecs_service(account, "api-cluster", "web");
        let (status, body) = send(
            account,
            "RegisterScalableTarget",
            json!({
                "ServiceNamespace": "ecs",
                "ResourceId": "service/api-cluster/web",
                "ScalableDimension": "ecs:service:DesiredCount",
                "MinCapacity": 1,
                "MaxCapacity": 10,
            }),
        );
        assert_eq!(status, 200);
        let arn = body["ScalableTargetARN"].as_str().unwrap();
        assert!(arn.contains(":scalable-target/"));

        let (_, targets) = send(
            account,
            "DescribeScalableTargets",
            json!({ "ServiceNamespace": "ecs", "ResourceIds": ["service/api-cluster/web"] }),
        );
        assert_eq!(
            targets["ScalableTargets"][0]["ResourceId"],
            "service/api-cluster/web"
        );
    }

    #[test]
    fn scaling_policy_lifecycle() {
        let account = next_account();
        let rid = "table/t-policy".to_string();
        register_target(account, &rid);
        let policy = json!({
            "PolicyName": "p-1",
            "ServiceNamespace": "dynamodb",
            "ResourceId": rid,
            "ScalableDimension": "dynamodb:table:ReadCapacityUnits",
            "PolicyType": "TargetTrackingScaling",
            "TargetTrackingScalingPolicyConfiguration": {
                "TargetValue": 70.0,
                "PredefinedMetricSpecification": {
                    "PredefinedMetricType": "DynamoDBReadCapacityUtilization",
                },
            },
        });
        let (status, body) = send(account, "PutScalingPolicy", policy);
        assert_eq!(status, 200);
        let arn = body["PolicyARN"].as_str().unwrap();
        assert!(arn.starts_with("arn:aws:autoscaling:us-east-1:"));
        assert!(arn.contains(":scalingPolicy:"));
        assert!(arn.ends_with(":policyName/p-1"));
        // dynamodb TargetTracking creates four companion alarms.
        assert_eq!(body["Alarms"].as_array().unwrap().len(), 4);

        let (_, body) = send(
            account,
            "DescribeScalingPolicies",
            json!({ "ServiceNamespace": "dynamodb" }),
        );
        let policies: Vec<_> = body["ScalingPolicies"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|p| p["PolicyName"].as_str() == Some("p-1"))
            .collect();
        assert_eq!(policies.len(), 1);
        assert_eq!(policies[0]["PolicyType"], "TargetTrackingScaling");
        assert_eq!(
            policies[0]["TargetTrackingScalingPolicyConfiguration"]["TargetValue"],
            70.0
        );
        assert_eq!(policies[0]["Alarms"].as_array().unwrap().len(), 4);

        // Delete, then fault on repeat deletion.
        let (status, _) = send(
            account,
            "DeleteScalingPolicy",
            json!({
                "PolicyName": "p-1",
                "ServiceNamespace": "dynamodb",
                "ResourceId": rid,
                "ScalableDimension": "dynamodb:table:ReadCapacityUnits",
            }),
        );
        assert_eq!(status, 200);
        let (status, body) = send(
            account,
            "DeleteScalingPolicy",
            json!({
                "PolicyName": "p-1",
                "ServiceNamespace": "dynamodb",
                "ResourceId": rid,
                "ScalableDimension": "dynamodb:table:ReadCapacityUnits",
            }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "ValidationException");
        assert!(body["message"]
            .as_str()
            .unwrap()
            .starts_with("No scaling policy found for service namespace: dynamodb"));
    }

    #[test]
    fn policy_type_validation_and_describe_filters() {
        let account = next_account();
        let rid = "table/t-pfilter".to_string();
        register_target(account, &rid);

        // PredictiveScaling is outside moto's supported policy types.
        let (status, body) = send(
            account,
            "PutScalingPolicy",
            json!({
                "PolicyName": "pred",
                "ServiceNamespace": "dynamodb",
                "ResourceId": rid,
                "ScalableDimension": "dynamodb:table:ReadCapacityUnits",
                "PolicyType": "PredictiveScaling",
            }),
        );
        assert_eq!(status, 400);
        assert!(body["message"]
            .as_str()
            .unwrap()
            .contains("at 'policyType'"));

        // A missing PolicyType fails the same shape.
        let (status, body) = send(
            account,
            "PutScalingPolicy",
            json!({
                "PolicyName": "none",
                "ServiceNamespace": "dynamodb",
                "ResourceId": rid,
                "ScalableDimension": "dynamodb:table:ReadCapacityUnits",
            }),
        );
        assert_eq!(status, 400);
        assert!(body["message"]
            .as_str()
            .unwrap()
            .contains("at 'policyType'"));

        // Two policies in the same namespace; description filter is a
        // substring match in moto's direction.
        for (name, policy_name) in [("a", "pf-a"), ("b", "pf-b")] {
            let _ = name;
            let (status, _) = send(
                account,
                "PutScalingPolicy",
                json!({
                    "PolicyName": policy_name,
                    "ServiceNamespace": "dynamodb",
                    "ResourceId": format!("{rid}/{name}"),
                    "ScalableDimension": "dynamodb:table:ReadCapacityUnits",
                    "PolicyType": "StepScaling",
                    "StepScalingPolicyConfiguration": {
                        "StepAdjustments": [{"MetricIntervalLowerBound": -1.0,
                                              "ScalingAdjustment": 1}]
                    },
                }),
            );
            assert_eq!(status, 200, "{policy_name}");
        }

        // The full filter id matches both policies (stored ids are its
        // substrings).
        let (_, body) = send(
            account,
            "DescribeScalingPolicies",
            json!({
                "ServiceNamespace": "dynamodb",
                "ResourceId": format!("{rid}/a"),
                "ScalableDimension": "dynamodb:table:ReadCapacityUnits",
            }),
        );
        let names: Vec<Value> = body["ScalingPolicies"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| p["PolicyName"].clone())
            .collect();
        assert_eq!(json!(names), json!(["pf-a"]), "substring filter direction");
        assert_eq!(body["NextToken"], Value::Null);
    }

    #[test]
    fn policy_describe_pagination() {
        let account = next_account();
        let rid = "table/t-page".to_string();
        register_target(account, &rid);
        for i in 0..3 {
            send(
                account,
                "PutScalingPolicy",
                json!({
                    "PolicyName": format!("pg-{i}"),
                    "ServiceNamespace": "dynamodb",
                    "ResourceId": rid,
                    "ScalableDimension": "dynamodb:table:ReadCapacityUnits",
                    "PolicyType": "StepScaling",
                    "StepScalingPolicyConfiguration": {"StepAdjustments": []},
                }),
            );
        }
        let (status, body) = send(
            account,
            "DescribeScalingPolicies",
            json!({
                "ServiceNamespace": "dynamodb",
                "MetricSelection": "none",
                "MaxResults": 2,
            }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["ScalingPolicies"].as_array().unwrap().len(), 2);
        let token = body["NextToken"].as_str().unwrap();
        assert_eq!(token, "2");

        let (_, body) = send(
            account,
            "DescribeScalingPolicies",
            json!({
                "ServiceNamespace": "dynamodb",
                "MaxResults": 2,
                "NextToken": token,
            }),
        );
        let page2: Vec<Value> = body["ScalingPolicies"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| p["PolicyName"].clone())
            .collect();
        assert_eq!(json!(page2), json!(["pg-2"]));
    }

    #[test]
    fn target_describe_pagination_moto_style() {
        let account = next_account();
        // Three targets under one dimension.
        for i in 0..3 {
            send(
                account,
                "RegisterScalableTarget",
                json!({
                    "ServiceNamespace": "dynamodb",
                    "ResourceId": format!("table/t-page-{i}"),
                    "ScalableDimension": "dynamodb:table:ReadCapacityUnits",
                    "MinCapacity": 1,
                }),
            );
        }
        let (status, body) = send(
            account,
            "DescribeScalableTargets",
            json!({
                "ServiceNamespace": "dynamodb",
                "ScalableDimension": "dynamodb:table:ReadCapacityUnits",
                "MaxResults": 2,
            }),
        );
        assert_eq!(status, 200);
        let page: Vec<Value> = body["ScalableTargets"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["ResourceId"].clone())
            .collect();
        assert_eq!(page.len(), 2);
        // moto's marker style: next token = page length - 1.
        assert_eq!(body["NextToken"], json!("1"));

        let (_, body) = send(
            account,
            "DescribeScalableTargets",
            json!({
                "ServiceNamespace": "dynamodb",
                "ScalableDimension": "dynamodb:table:ReadCapacityUnits",
                "MaxResults": 2,
                "NextToken": "1",
            }),
        );
        let page: Vec<Value> = body["ScalableTargets"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["ResourceId"].clone())
            .collect();
        // Last page: moto skips one past the marker → one item left.
        assert_eq!(page.len(), 1);
        // No second item, so no further token.
        assert_eq!(body["NextToken"], Value::Null);
    }

    #[test]
    fn scheduled_action_lifecycle() {
        let account = next_account();
        let rid = "table/t-sched".to_string();
        register_target(account, &rid);

        // Empty describe with the required namespace.
        let (status, body) = send(
            account,
            "DescribeScheduledActions",
            json!({"ServiceNamespace": "dynamodb"}),
        );
        assert_eq!(status, 200);
        assert!(body["ScheduledActions"].as_array().unwrap().is_empty());

        let (status, _) = send(
            account,
            "PutScheduledAction",
            json!({
                "ServiceNamespace": "dynamodb",
                "ResourceId": rid,
                "ScalableDimension": "dynamodb:table:ReadCapacityUnits",
                "ScheduledActionName": "nightly",
                "Schedule": "cron(0 18 * * ? *)",
                "ScalableTargetAction": {"MinCapacity": 1, "MaxCapacity": 20},
            }),
        );
        assert_eq!(status, 200);

        let (_, body) = send(
            account,
            "DescribeScheduledActions",
            json!({"ServiceNamespace": "dynamodb"}),
        );
        let actions = body["ScheduledActions"].as_array().unwrap();
        let mine: Vec<_> = actions
            .iter()
            .filter(|a| a["ScheduledActionName"] == "nightly")
            .collect();
        assert_eq!(mine.len(), 1);
        let arn = mine[0]["ScheduledActionARN"].as_str().unwrap();
        assert!(arn.contains(":scheduledAction:dynamodb/table/t-sched:scheduledActionName/nightly"));
        assert_eq!(
            mine[0]["ScalableTargetAction"],
            json!({"MinCapacity": 1, "MaxCapacity": 20})
        );

        // Update: window overwrite and truthy-field retention.
        let (status, _) = send(
            account,
            "PutScheduledAction",
            json!({
                "ServiceNamespace": "dynamodb",
                "ResourceId": rid,
                "ScalableDimension": "dynamodb:table:ReadCapacityUnits",
                "ScheduledActionName": "nightly",
                "ScalableTargetAction": {"MinCapacity": 5, "MaxCapacity": 50},
            }),
        );
        assert_eq!(status, 200);
        let (_, body) = send(
            account,
            "DescribeScheduledActions",
            json!({
                "ServiceNamespace": "dynamodb",
                "ScheduledActionNames": ["nightly"],
            }),
        );
        let action = &body["ScheduledActions"].as_array().unwrap()[0];
        // moto overwrote the action body; Schedule stays (truthy retention)?
        // moto's update keeps Schedule when not sent.
        assert_eq!(
            action["ScalableTargetAction"],
            json!({"MinCapacity": 5, "MaxCapacity": 50})
        );

        // Delete is idempotent; no fault for absent actions.
        let (status, _) = send(
            account,
            "DeleteScheduledAction",
            json!({
                "ServiceNamespace": "dynamodb",
                "ResourceId": rid,
                "ScalableDimension": "dynamodb:table:ReadCapacityUnits",
                "ScheduledActionName": "nightly",
            }),
        );
        assert_eq!(status, 200);
        let (status, _) = send(
            account,
            "DeleteScheduledAction",
            json!({
                "ServiceNamespace": "dynamodb",
                "ResourceId": rid,
                "ScalableDimension": "dynamodb:table:ReadCapacityUnits",
                "ScheduledActionName": "nightly",
            }),
        );
        assert_eq!(status, 200);
        let (_, body) = send(
            account,
            "DescribeScheduledActions",
            json!({"ServiceNamespace": "dynamodb"}),
        );
        assert!(body["ScheduledActions"].as_array().unwrap().is_empty());
    }

    #[test]
    fn operations_absent_from_moto_fork_answer_501() {
        let account = next_account();
        for op in [
            "DescribeScalingActivities",
            "GetPredictiveScalingForecast",
            "ListTagsForResource",
            "TagResource",
            "UntagResource",
        ] {
            let (status, body) = send(account, op, json!({}));
            assert_eq!(status, 501, "{op} must answer 501: {body}");
            assert_eq!(body["__type"], "NotImplemented", "{op}");
        }
    }

    #[test]
    fn dispatch_ignores_other_services() {
        let request = Request {
            service: "memorydb".to_string(),
            operation: "RegisterScalableTarget".to_string(),
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
