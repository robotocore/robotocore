//! Auto Scaling backend — query-XML protocol port of the moto `autoscaling`
//! service.
//!
//! Wire contract: AWS query protocol, XML namespace
//! `http://autoscaling.amazonaws.com/doc/2011-01-01/`, form bodies,
//! `<{Op}Response xmlns=…><{Op}Result>…</{Op}Result><ResponseMetadata/>`
//! envelopes, with every result rendered in the botocore output-shape member
//! order the vendored moto serializer emits (ground truth captured
//! operation-by-operation against the deployed fork checkout
//! `moto/autoscaling`, byte-identical to moto 5.1.23).
//!
//! Semantics mirror the fork exactly:
//! - all 56 routed handlers in the fork's `responses.py` are implemented 1:1;
//!   `LaunchInstances` has no fork route/handler and stays a bare
//!   `501 NotImplemented` (the compat suite accepts that ClientError).
//! - the 8 probe rows marked `not_implemented` (Attach/Describe/Detach
//!   TrafficSources, CompleteLifecycleAction, DisableMetricsCollection,
//!   GetPredictiveScalingForecast, RecordLifecycleActionHeartbeat,
//!   RollbackInstanceRefresh) all have fork handlers and are implemented per
//!   the wave-2 parent ruling in `docs/moto-parity-progress.md` (fork wins
//!   over stale probes).
//! - the probe's 22 `500_error` rows were moto crashes (`KeyError` /
//!   `IndexError`/`StopIteration` on missing groups and instances). Per the
//!   datapipeline precedent those paths answer the modeled `ValidationError`
//!   (400) instead of reproducing a crash; `ResourceContention` stays the
//!   fork's modeled-but-unrouted ServiceException shape (HTTP 500 with a
//!   doubled lowercase `<message>` element, captured verbatim).
//! - EC2 integration is live: creating/scaling a group launches instances
//!   through the EC2 backend (loopback `RunInstances`), tags them with
//!   `aws:autoscaling:groupName`, propagates launch-time group tags, and
//!   terminates them on scale-in, detach, group delete, and
//!   `TerminateInstanceInAutoScalingGroup` — moto's exact flow through
//!   `ec2_backend.run_instances`.
//! - classic-ELB/ELBv2 registration mirrors moto's `update_attached_elbs` /
//!   `update_attached_target_groups` on every capacity change, best-effort
//!   (moto silently skips deleted balancers via its caught
//!   `LoadBalancerNotFoundError`).
//! - Deliberate quirk: `attach_instances` with a nonexisting instance id
//!   answers EC2's `InvalidInstanceID.NotFound` (that crash crossed the EC2
//!   boundary in moto too). `UserData` on launch configurations is echoed as
//!   received, without moto's accidental double-base64 (no test asserts it).

use super::elbv2::{
    encoded_pairs, p_bool, p_i64, p_list, p_string, p_struct_list, parse_params, ElbVal,
    LoopbackBody,
};
use crate::services::ec2;
use crate::store;
use crate::{Request, Response};
use std::collections::BTreeMap;

const XMLNS: &str = "http://autoscaling.amazonaws.com/doc/2011-01-01/";

/// moto's cooldown default (models.py `DEFAULT_COOLDOWN`).
const DEFAULT_COOLDOWN: i64 = 300;
/// moto's `ASG_NAME_TAG` — the EC2 tag linking an instance to its group.
const ASG_NAME_TAG: &str = "aws:autoscaling:groupName";
/// Fixed policy-ARN uuid baked into moto's `FakeScalingPolicy.arn`.
const POLICY_ARN_UUID: &str = "c322761b-3172-4d56-9a21-0ed9d6161d67";
/// Fixed launch-configuration ARN uuid baked into moto's model.
const LC_ARN_UUID: &str = "9dbbbf87-6141-428a-a409-0752edbe6cad";

type Params = BTreeMap<String, ElbVal>;
type OpResult<T> = Result<T, AsgError>;

/// A modeled autoscaling error: HTTP status, code, message, and whether moto
/// renders the doubled lowercase `<message>` element (unmodeled
/// ServiceExceptions like `ResourceContention` emit both spellings).
#[derive(Debug, Clone)]
struct AsgError {
    status: u16,
    code: String,
    message: String,
    doubled_message: bool,
}

impl AsgError {
    fn new(status: u16, code: &str, message: impl Into<String>) -> Self {
        Self {
            status,
            code: code.to_string(),
            message: message.into(),
            doubled_message: false,
        }
    }
}

/// moto `ValidationError` (HTTP 400) — also the answer moto gives as modelled
/// behavior where its python crashed (KeyError → 500 probe rows).
fn validation(message: impl Into<String>) -> AsgError {
    AsgError::new(400, "ValidationError", message)
}

/// The fork's standard not-found wording.
fn group_not_found(name: &str) -> AsgError {
    validation(format!(
        "AutoScalingGroup name not found - AutoScalingGroup '{name}' not found"
    ))
}

/// moto `ResourceContentionError` — modeled ServiceException but not part of
/// the service spec's shapes, so moto renders Code+Message+message at HTTP 500.
fn resource_contention() -> AsgError {
    AsgError {
        status: 500,
        code: "ResourceContention".into(),
        message: "You already have a pending update to an Auto Scaling resource \
                  (for example, a group, instance, or load balancer)."
            .into(),
        doubled_message: true,
    }
}

/// moto `InvalidInstanceError` (a ValidationError subclass).
fn invalid_instance(instance_id: &str) -> AsgError {
    validation(format!("Instance [{instance_id}] is invalid."))
}

/// The gateway/sidecar fallthrough for unrouted actions (`LaunchInstances`).
fn not_implemented(op: &str) -> AsgError {
    AsgError::new(
        501,
        "NotImplemented",
        format!("The {op} action has not been implemented"),
    )
}

// ---------------------------------------------------------------------------
// Timestamps (moto's TimestampSerializer renders aware-utc iso8601 with
// microseconds; cause strings embed python's str(datetime), space separator).
// ---------------------------------------------------------------------------

fn now_wire_ts() -> String {
    chrono::Utc::now()
        .format("%Y-%m-%dT%H:%M:%S%.6fZ")
        .to_string()
}

fn now_moto_str() -> String {
    chrono::Utc::now()
        .format("%Y-%m-%d %H:%M:%S%.6f")
        .to_string()
}

fn partition(region: &str) -> &'static str {
    if region.starts_with("cn-") {
        "aws-cn"
    } else if region.starts_with("us-gov") {
        "aws-us-gov"
    } else {
        "aws"
    }
}

/// Python `str(float)` formatting (trailing `.0` on whole doubles).
fn float_str(raw: &str) -> String {
    match raw.trim().parse::<f64>() {
        Ok(v) if v.fract() == 0.0 && v.abs() < 1e12 => format!("{:.1}", v),
        Ok(v) => format!("{v}"),
        Err(_) => raw.to_string(),
    }
}

/// Python `int()` coercion for integer-typed members.
fn int_str(raw: &str) -> String {
    raw.trim()
        .parse::<i64>()
        .map(|v| v.to_string())
        .unwrap_or_else(|_| raw.to_string())
}

// ---------------------------------------------------------------------------
// State: per-(account, region) backend, mirroring moto's AutoScalingBackend.
// ---------------------------------------------------------------------------

/// One group-member instance (moto `InstanceState`).
#[derive(Clone, Debug)]
struct AsgInstance {
    instance_id: String,
    az: String,
    instance_type: String,
    lifecycle_state: String,
    health_status: String,
    protected_from_scale_in: bool,
}

impl AsgInstance {
    fn in_service(
        instance_id: String,
        az: String,
        instance_type: String,
        is_protected: bool,
    ) -> Self {
        Self {
            instance_id,
            az,
            instance_type,
            lifecycle_state: "InService".into(),
            health_status: "Healthy".into(),
            protected_from_scale_in: is_protected,
        }
    }
}

/// moto `FakeWarmPool` (capacity fields kept raw-string like moto's params).
#[derive(Clone, Debug)]
struct WarmPool {
    max_group_prepared_capacity: Option<String>,
    min_size: String,
    pool_state: String,
    instance_reuse_policy: Option<Params>,
}

/// moto `FakeLaunchConfiguration`.
#[derive(Clone, Debug)]
struct LaunchConfig {
    name: String,
    image_id: String,
    key_name: Option<String>,
    ramdisk_id: Option<String>,
    kernel_id: Option<String>,
    security_groups: Vec<String>,
    user_data: Option<String>,
    instance_type: String,
    instance_monitoring_enabled: bool,
    iam_instance_profile: Option<String>,
    spot_price: Option<String>,
    ebs_optimized: bool,
    associate_public_ip_address: bool,
    /// Parsed `BlockDeviceMappings.member.N`.
    block_device_mappings: Vec<Params>,
    metadata_options: Option<Params>,
    classic_link_vpc_id: Option<String>,
    classic_link_vpc_security_groups: Vec<String>,
    arn: String,
    created_time: String,
}

/// moto `FakeScheduledAction`.
#[derive(Clone, Debug)]
struct ScheduledAction {
    auto_scaling_group_name: String,
    desired_capacity: Option<i64>,
    max_size: Option<i64>,
    min_size: Option<i64>,
    scheduled_action_name: String,
    start_time: Option<String>,
    end_time: Option<String>,
    recurrence: Option<String>,
    time_zone: Option<String>,
}

/// moto `FakeScalingPolicy`.
#[derive(Clone, Debug)]
struct ScalingPolicy {
    name: String,
    policy_type: String,
    metric_aggregation_type: Option<String>,
    adjustment_type: Option<String>,
    auto_scaling_group_name: String,
    min_adjustment_magnitude: Option<String>,
    scaling_adjustment: Option<i64>,
    cooldown: Option<i64>,
    target_tracking_config: Option<Params>,
    step_adjustments: Vec<Params>,
    estimated_instance_warmup: Option<String>,
    predictive_scaling_configuration: Option<Params>,
}

/// moto `LifecycleHook` (template-only fields keep moto's hardcoded values).
#[derive(Clone, Debug)]
struct LifecycleHook {
    name: String,
    auto_scaling_group_name: String,
    lifecycle_transition: Option<String>,
    heartbeat_timeout: i64,
    default_result: String,
}

/// moto `NotificationConfiguration`.
#[derive(Clone, Debug)]
struct NotificationConfig {
    auto_scaling_group_name: String,
    topic_arn: String,
    notification_types: Vec<String>,
}

/// moto `InstanceRefresh`.
#[derive(Clone, Debug)]
struct InstanceRefresh {
    instance_refresh_id: String,
    auto_scaling_group_name: String,
    status: String,
    start_time: String,
    end_time: Option<String>,
    preferences: Option<Params>,
}

/// A stored group tag; the tags setter fills missing identity fields.
#[derive(Clone, Debug)]
struct TagRecord {
    resource_id: String,
    resource_type: String,
    key: String,
    value: Option<String>,
    propagate: bool,
}

/// moto `FakeAutoScalingGroup`.
#[derive(Clone, Debug)]
struct Group {
    name: String,
    id: String,
    region: String,
    account_id: u64,
    availability_zones: Vec<String>,
    vpc_zone_identifier: Option<String>,
    max_size: Option<i64>,
    min_size: Option<i64>,
    desired_capacity: i64,
    mixed_instances_policy: Option<Params>,
    launch_config_name: Option<String>,
    launch_template_id: Option<String>,
    launch_template_name: Option<String>,
    provided_launch_template_version: Option<String>,
    /// Snapshot of the resolved launch surface for EC2 launches (moto walks
    /// the launch config / template version at each run_instances call).
    launch_image_id: Option<String>,
    launch_instance_type: Option<String>,
    launch_key_name: Option<String>,
    launch_user_data: Option<String>,
    default_cooldown: i64,
    health_check_grace_period: Option<i64>,
    health_check_type: String,
    load_balancer_names: Vec<String>,
    target_group_arns: Vec<String>,
    placement_group: Option<String>,
    termination_policies: Vec<String>,
    capacity_rebalance: bool,
    new_instances_protected_from_scale_in: bool,
    suspended_processes: Vec<String>,
    instance_states: Vec<AsgInstance>,
    tags: Vec<TagRecord>,
    metrics: Vec<String>,
    warm_pool: Option<WarmPool>,
    traffic_sources: Vec<Params>,
    created_time: String,
}

impl Group {
    fn arn(&self) -> String {
        format!(
            "arn:{}:autoscaling:{}:{}:autoScalingGroup:{}:autoScalingGroupName/{}",
            partition(&self.region),
            self.region,
            self.account_id,
            self.id,
            self.name
        )
    }

    fn service_linked_role_arn(&self) -> String {
        format!(
            "arn:{}:iam::{}:role/aws-service-role/autoscaling.amazonaws.com/AWSServiceRoleForAutoScaling",
            partition(&self.region),
            self.account_id
        )
    }

    fn active_instances(&self) -> Vec<&AsgInstance> {
        self.instance_states
            .iter()
            .filter(|x| x.lifecycle_state == "InService")
            .collect()
    }

    /// moto `get_propagated_tags` — tags marked PropagateAtLaunch.
    fn propagated_tags(&self) -> Vec<(String, String)> {
        self.tags
            .iter()
            .filter(|t| t.propagate)
            .map(|t| (t.key.clone(), t.value.clone().unwrap_or_default()))
            .collect()
    }
}

/// Per-(account, region) backend state.
#[derive(Clone, Debug, Default)]
pub struct AsgState {
    autoscaling_groups: BTreeMap<String, Group>,
    launch_configurations: BTreeMap<String, LaunchConfig>,
    scheduled_actions: BTreeMap<String, ScheduledAction>,
    policies: BTreeMap<String, ScalingPolicy>,
    lifecycle_hooks: BTreeMap<String, LifecycleHook>,
    notification_configurations: Vec<NotificationConfig>,
    instance_refreshes: BTreeMap<String, Vec<InstanceRefresh>>,
}

/// moto's scaling-process catalog (order from models.py).
const PROCESS_NAMES: [&str; 9] = [
    "Launch",
    "Terminate",
    "AddToLoadBalancer",
    "AlarmNotification",
    "AZRebalance",
    "HealthCheck",
    "InstanceRefresh",
    "ReplaceUnhealthy",
    "ScheduledActions",
];

// ---------------------------------------------------------------------------
// XML writing helpers.
// ---------------------------------------------------------------------------

fn tag_esc(name: &str, body: &str) -> String {
    format!("<{name}>{}</{name}>", esc(body))
}

/// `<Name/>` when absent, `<Name></Name>` for an empty string.
fn tag_opt(name: &str, body: Option<&str>) -> String {
    match body {
        None => format!("<{name}/>"),
        Some(b) => format!("<{name}>{}</{name}>", esc(b)),
    }
}

fn tag_raw(name: &str, body: &str) -> String {
    if body.is_empty() {
        format!("<{name}/>")
    } else {
        format!("<{name}>{body}</{name}>")
    }
}

fn esc(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            c => out.push(c),
        }
    }
    out
}

/// Wrapped list wrapper: `<Name/>` when empty.
fn list_xml(name: &str, members: &[String]) -> String {
    let inner: String = members
        .iter()
        .map(|m| format!("<member>{m}</member>"))
        .collect();
    tag_raw(name, &inner)
}

fn bool_text(v: bool) -> &'static str {
    if v {
        "true"
    } else {
        "false"
    }
}

/// Render a parsed (key-sorted) nested struct in member order.
fn param_members(m: &BTreeMap<String, ElbVal>) -> String {
    let mut out = String::new();
    for (k, v) in m {
        out.push_str(&param_member(k, v));
    }
    out
}

/// Render one struct member following the botocore member name.
fn param_member(key: &str, v: &ElbVal) -> String {
    match v {
        ElbVal::S(s) => tag_esc(key, s),
        ElbVal::L(items) => {
            let members: String = items
                .iter()
                .map(|i| format!("<member>{}</member>", param_member(key, i)))
                .collect();
            tag_raw(key, &members)
        }
        ElbVal::M(m) => tag_raw(key, &param_members(m)),
        ElbVal::None => format!("<{key}/>"),
    }
}

// ---------------------------------------------------------------------------
// Error and result envelopes (moto's QuerySerializer paths).
// ---------------------------------------------------------------------------

fn error_response(err: &AsgError) -> Response {
    let doubled = if err.doubled_message {
        format!("<message>{}</message>", esc(&err.message))
    } else {
        String::new()
    };
    Response::xml(
        err.status,
        format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <ErrorResponse xmlns=\"{XMLNS}\"><Error><Type>Sender</Type>\
             <Code>{}</Code><Message>{}</Message>{doubled}</Error>\
             <RequestId>{}</RequestId></ErrorResponse>",
            esc(&err.code),
            esc(&err.message),
            uuid::Uuid::new_v4()
        )
        .into_bytes(),
    )
}

fn result_response(op: &str, body: &str) -> Response {
    let result = if body.is_empty() {
        format!("<{op}Result/>")
    } else {
        format!("<{op}Result>{body}</{op}Result>")
    };
    Response::xml(
        200,
        format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <{op}Response xmlns=\"{XMLNS}\">{result}\
             <ResponseMetadata><RequestId>{}</RequestId></ResponseMetadata>\
             </{op}Response>",
            uuid::Uuid::new_v4()
        )
        .into_bytes(),
    )
}

// ---------------------------------------------------------------------------
// EC2 bridge (moto's `from moto.ec2 import ec2_backends` dependency).
// Loopback HTTP calls from the ASG backend into the EC2 backend, exactly
// moto's same-process backend calls, plus direct EC2-state reads like the
// memorydb subnet bridge.
// ---------------------------------------------------------------------------

/// A live EC2 instance view (subset the ASG render consumes).
struct Ec2InstanceView {
    az: String,
    instance_type: String,
    key_name: Option<String>,
    security_group_names: Vec<String>,
    image_id: String,
    user_data: Option<String>,
}

/// Touch the EC2 scope (moto seeds the default VPC at backend construction)
/// and read one instance.
fn ec2_get_instance(req: &Request, instance_id: &str) -> Option<Ec2InstanceView> {
    let slot = ec2::state::shared();
    let mut map = slot.write();
    let st = map
        .entry((req.account, req.region.clone()))
        .or_insert_with(|| {
            let mut s = ec2::state::Ec2State::default();
            s.ensure_default_vpc(&req.region, req.account);
            s
        });
    let inst = st.instances.iter().find(|i| i.id == instance_id)?;
    Some(Ec2InstanceView {
        az: inst.az.clone(),
        instance_type: inst.instance_type.clone(),
        key_name: inst.key_name.clone(),
        security_group_names: inst
            .security_group_ids
            .iter()
            .filter_map(|id| {
                st.security_groups
                    .iter()
                    .find(|sg| &sg.id == id)
                    .map(|sg| sg.name.clone())
            })
            .collect(),
        image_id: inst.image_id.clone(),
        user_data: inst.user_data.clone(),
    })
}

/// Look up a launch template by id or name in the EC2 state (moto keeps this
/// dependency for `_set_launch_configuration`).
fn ec2_launch_template(
    req: &Request,
    template_id: &str,
    template_name: &str,
) -> Option<(String, String, i64)> {
    let slot = ec2::state::shared();
    let mut map = slot.write();
    let st = map
        .entry((req.account, req.region.clone()))
        .or_insert_with(|| {
            let mut s = ec2::state::Ec2State::default();
            s.ensure_default_vpc(&req.region, req.account);
            s
        });
    let pos = if !template_id.is_empty() {
        st.launch_templates.iter().position(|l| l.id == template_id)
    } else {
        st.launch_templates
            .iter()
            .position(|l| l.name == template_name)
    }?;
    let lt = &st.launch_templates[pos];
    Some((lt.id.clone(), lt.name.clone(), lt.default_version))
}

/// First `<tag>value` under `text` (unescaped like elbv2's grab_tag).
fn xml_tag(text: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let start = text.find(&open)? + open.len();
    let rest = &text[start..];
    let end = rest.find(&format!("</{tag}>"))?;
    Some(
        rest[..end]
            .replace("&lt;", "<")
            .replace("&gt;", ">")
            .replace("&quot;", "\"")
            .replace("&#39;", "'")
            .replace("&amp;", "&"),
    )
}

/// Loopback call with this request's region/account in the credential scope,
/// so cross-service state lands in the correct (account, region) scope.
fn cross_call(req: &Request, service: &'static str, body: &LoopbackBody) -> Option<(u16, String)> {
    use std::io::{Read, Write};
    use std::net::TcpStream;

    // No Host header means no addressable twin: skip the loopback call
    // (and log) rather than dialing a hardcoded dead port — the ASG then
    // records zero launched instances instead of wedging.
    let host = req.headers.get("host").cloned();
    let Some(host) = &host else {
        eprintln!("asg loopback: request has no Host header; skipping cross-call to {service}");
        return None;
    };
    let (content_type, body_str, target) = match body {
        LoopbackBody::Form(b) => ("application/x-www-form-urlencoded", b.clone(), None),
        LoopbackBody::Json { target, payload } => (
            "application/x-amz-json-1.1",
            payload.clone(),
            Some(target.clone()),
        ),
    };
    let mut headers = format!(
        "POST / HTTP/1.1\r\nHost: {host}\r\nContent-Type: {ct}\r\n\
         Authorization: AWS4-HMAC-SHA256 Credential={acct:012}/20240101/{region}/{svc}/aws4_request\r\n\
         X-Amz-Date: 20240101T000000Z\r\nContent-Length: {len}\r\nConnection: close\r\n",
        ct = content_type,
        acct = req.account,
        region = req.region,
        svc = service,
        len = body_str.len(),
    );
    if let Some(t) = target {
        headers.push_str(&format!("X-Amz-Target: {t}\r\n"));
    }
    let wire = format!("{headers}\r\n{body_str}");
    let mut stream = TcpStream::connect(host.as_str()).ok()?;
    let _ = stream.set_read_timeout(Some(std::time::Duration::from_secs(3)));
    let _ = stream.write_all(wire.as_bytes());
    let mut buf = String::new();
    let _ = stream.read_to_string(&mut buf);
    let mut parts = buf.splitn(2, "\r\n\r\n");
    let head = parts.next().unwrap_or_default();
    let body_out = parts.next().unwrap_or_default().to_string();
    let status: u16 = head
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    Some((status, body_out))
}

fn ec2_call(req: &Request, form_body: &str) -> Option<(u16, String)> {
    cross_call(req, "ec2", &LoopbackBody::Form(form_body.to_string()))
}

/// moto picks a random AZ per launch; with mocks we use the group's first AZ.
fn pick_az(group: &Group) -> String {
    group
        .availability_zones
        .first()
        .cloned()
        .unwrap_or_default()
}

/// Launch EC2 instances for the group (moto's
/// `replace_autoscaling_group_instances`); returns the live EC2 state per id.
fn launch_instances(req: &Request, group: &Group, count: usize) -> Vec<AsgInstance> {
    if count == 0 {
        return Vec::new();
    }
    let Some(image_id) = group.launch_image_id.clone() else {
        return Vec::new();
    };
    let instance_type_default = group.launch_instance_type.clone().unwrap_or_default();
    let mut pairs: Vec<(String, String)> = vec![
        ("Action".into(), "RunInstances".into()),
        ("ImageId".into(), image_id),
        ("MinCount".into(), count.to_string()),
        ("MaxCount".into(), count.to_string()),
        ("InstanceType".into(), instance_type_default.clone()),
        ("Placement.AvailabilityZone".into(), pick_az(group)),
    ];
    if let Some(k) = &group.launch_key_name {
        pairs.push(("KeyName".into(), k.clone()));
    }
    if let Some(u) = &group.launch_user_data {
        pairs.push(("UserData".into(), u.clone()));
    }
    let subnet_id = group
        .vpc_zone_identifier
        .as_deref()
        .and_then(|v| v.split(',').next())
        .unwrap_or_default();
    if !subnet_id.is_empty() {
        pairs.push(("SubnetId".into(), subnet_id.to_string()));
    }
    let mut admin_tags: Vec<(String, String)> = vec![(ASG_NAME_TAG.into(), group.name.clone())];
    for (k, v) in group.propagated_tags() {
        if !admin_tags.iter().any(|(ek, _)| ek == &k) {
            admin_tags.push((k, v));
        }
    }
    pairs.push(("TagSpecification.1.ResourceType".into(), "instance".into()));
    for (i, (k, v)) in admin_tags.iter().enumerate() {
        pairs.push((format!("TagSpecification.1.Tag.{}.Key", i + 1), k.clone()));
        pairs.push((format!("TagSpecification.1.Tag.{}.Value", i + 1), v.clone()));
    }
    let Some((200, xml)) = ec2_call(req, &encoded_pairs(&pairs)) else {
        return Vec::new();
    };
    xml.split("<item>")
        .filter_map(|chunk| {
            let id = xml_tag(chunk, "instanceId")?;
            let instance_type = xml_tag(chunk, "instanceType")
                .filter(|t| !t.is_empty())
                .unwrap_or_else(|| instance_type_default.clone());
            let az = xml_tag(chunk, "availabilityZone")
                .filter(|a| !a.is_empty())
                .unwrap_or_else(|| pick_az(group));
            Some(AsgInstance::in_service(
                id,
                az,
                instance_type,
                group.new_instances_protected_from_scale_in,
            ))
        })
        .collect()
}

/// EC2 `TerminateInstances` (moto's scale-in / delete / terminate path).
fn terminate_ec2(req: &Request, ids: &[String]) {
    if ids.is_empty() {
        return;
    }
    let mut pairs: Vec<(String, String)> =
        vec![("Action".to_string(), "TerminateInstances".into())];
    for (i, id) in ids.iter().enumerate() {
        pairs.push((format!("InstanceId.{}", i + 1), id.clone()));
    }
    let _ = ec2_call(req, &encoded_pairs(&pairs));
}

/// EC2 `CreateTags` (moto's attach_instances marks the instance with the ASG
/// name tag).
fn create_tags_ec2(req: &Request, pairs: Vec<(String, String)>) {
    let _ = ec2_call(req, &encoded_pairs(&pairs));
}

/// EC2 `DeleteTags` for the ASG-name mark (moto's detach_instances).
fn delete_asg_tag_ec2(req: &Request, instance_id: &str, group_name: &str) {
    let pairs = vec![
        ("Action".into(), "DeleteTags".into()),
        ("ResourceId.1".into(), instance_id.to_string()),
        ("Tag.1.Key".into(), ASG_NAME_TAG.into()),
        ("Tag.1.Value".into(), group_name.to_string()),
    ];
    let _ = ec2_call(req, &encoded_pairs(&pairs));
}

// ---------------------------------------------------------------------------
// ELB (classic) and ELBv2 mirrors of moto's update_attached_elbs /
// update_attached_target_groups — best-effort, silently skipped like moto's
// caught LoadBalancerNotFoundError when the balancer no longer exists.
// ---------------------------------------------------------------------------

fn sync_attached_elbs(req: &Request, group: &Group) {
    if group.load_balancer_names.is_empty() {
        return;
    }
    let mut names: Vec<(String, String)> = vec![("Action".into(), "DescribeLoadBalancers".into())];
    for (i, n) in group.load_balancer_names.iter().enumerate() {
        names.push((format!("LoadBalancerNames.member.{}", i + 1), n.clone()));
    }
    let Some((200, _)) = cross_call(req, "elb", &LoopbackBody::Form(encoded_pairs(&names))) else {
        return;
    };
    let ids: Vec<String> = group
        .active_instances()
        .into_iter()
        .map(|x| x.instance_id.clone())
        .collect();
    if ids.is_empty() {
        return;
    }
    for elb in &group.load_balancer_names {
        let mut pairs: Vec<(String, String)> = vec![
            ("Action".into(), "RegisterInstancesWithLoadBalancer".into()),
            ("LoadBalancerName".into(), elb.clone()),
        ];
        for (i, id) in ids.iter().enumerate() {
            pairs.push((format!("Instances.member.{}.InstanceId", i + 1), id.clone()));
        }
        let _ = cross_call(req, "elb", &LoopbackBody::Form(encoded_pairs(&pairs)));
    }
}

fn deregister_from_elbs(req: &Request, elb_names: &[String], ids: &[String]) {
    if ids.is_empty() {
        return;
    }
    for elb in elb_names {
        let mut pairs: Vec<(String, String)> = vec![
            (
                "Action".into(),
                "DeregisterInstancesFromLoadBalancer".into(),
            ),
            ("LoadBalancerName".into(), elb.clone()),
        ];
        for (i, id) in ids.iter().enumerate() {
            pairs.push((format!("Instances.member.{}.InstanceId", i + 1), id.clone()));
        }
        let _ = cross_call(req, "elb", &LoopbackBody::Form(encoded_pairs(&pairs)));
    }
}

fn sync_target_groups(req: &Request, group: &Group) {
    if group.target_group_arns.is_empty() {
        return;
    }
    // moto master e70a7315 (#10412): target groups can be deleted before
    // their Auto Scaling group. Describe each attached ARN separately and
    // keep refreshing the surviving groups — a vanished target group must
    // neither wedge the scale-down/delete nor suppress registration on the
    // groups that still exist (moto's update_attached_target_groups prunes
    // the deleted ARNs before describing).
    let ids: Vec<String> = group
        .instance_states
        .iter()
        .map(|x| x.instance_id.clone())
        .collect();
    for attached in group.target_group_arns.clone() {
        let describe: Vec<(String, String)> = vec![
            ("Action".into(), "DescribeTargetGroups".into()),
            ("TargetGroupArn".into(), attached.clone()),
        ];
        let Some((200, xml)) =
            cross_call(req, "elbv2", &LoopbackBody::Form(encoded_pairs(&describe)))
        else {
            continue;
        };
        // Only refresh the ARN that was asked for: moto describes the pruned
        // remainder, never the delete markers of other callers' groups.
        let Some(tg_arn) = xml
            .split("<member>")
            .find_map(|chunk| xml_tag(chunk, "TargetGroupArn"))
            .filter(|arn| arn == &attached)
        else {
            continue;
        };
        let port = xml_tag(&xml, "Port").unwrap_or_default();
        let mut pairs: Vec<(String, String)> = vec![
            ("Action".into(), "RegisterTargets".into()),
            ("TargetGroupArn".into(), tg_arn),
        ];
        for (i, id) in ids.iter().enumerate() {
            pairs.push((format!("Targets.member.{}.Id", i + 1), id.clone()));
            if !port.is_empty() {
                pairs.push((format!("Targets.member.{}.Port", i + 1), port.clone()));
            }
        }
        let _ = cross_call(req, "elbv2", &LoopbackBody::Form(encoded_pairs(&pairs)));
    }
}

fn deny_deregister_target_groups(req: &Request, group: &Group, target_group_arns: &[String]) {
    if group.instance_states.is_empty() {
        return;
    }
    for tg_arn in target_group_arns {
        let mut pairs: Vec<(String, String)> = vec![
            ("Action".into(), "DeregisterTargets".into()),
            ("TargetGroupArn".into(), tg_arn.clone()),
        ];
        for (i, inst) in group.instance_states.iter().enumerate() {
            pairs.push((
                format!("Targets.member.{}.Id", i + 1),
                inst.instance_id.clone(),
            ));
        }
        let _ = cross_call(req, "elbv2", &LoopbackBody::Form(encoded_pairs(&pairs)));
    }
}

// ---------------------------------------------------------------------------
// Backend map access.
// ---------------------------------------------------------------------------

/// Apply `f` with the scoped backend state.
fn with_state<T>(req: &Request, f: impl FnOnce(&mut AsgState) -> T) -> T {
    let slots = store::slot::<AsgState>("autoscaling");
    let mut map = slots.write();
    let state = map.entry((req.account, req.region.clone())).or_default();
    f(state)
}

/// Resolve the action (gateway sets `operation`; fall back to the form).
fn action_of(req: &Request) -> Option<String> {
    if !req.operation.is_empty() {
        return Some(req.operation.trim().to_string());
    }
    for source in [
        req.query_string.as_str(),
        std::str::from_utf8(&req.body).unwrap_or_default(),
    ] {
        if let Some(action) = crate::proto::query_map(source)
            .get("Action")
            .and_then(|v| v.first())
        {
            return Some(action.clone());
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Capacity math (moto's FakeAutoScalingGroup.set_desired_capacity).
// ---------------------------------------------------------------------------

/// ceil(a / b) for non-negative a and positive b (math.ceil semantics).
fn ceil_div(a: i64, b: i64) -> i64 {
    if b <= 0 {
        return a.max(0);
    }
    (a + b - 1).div_euclid(b)
}

/// moto: `int(weight) if weight.isdigit() else 1`, lifted from 0 to 1.
fn primary_weight_of(overrides: &[Params]) -> i64 {
    let raw = overrides
        .first()
        .and_then(|o| elb_string(o, "WeightedCapacity"))
        .unwrap_or("1");
    let v = raw.trim();
    if !v.is_empty() && v.chars().all(|c| c.is_ascii_digit()) {
        (v.parse::<i64>().unwrap_or(1)).max(1)
    } else {
        1
    }
}

fn elb_string<'a>(m: &'a BTreeMap<String, ElbVal>, key: &str) -> Option<&'a str> {
    m.get(key).and_then(ElbVal::string)
}

fn elb_i64(m: &BTreeMap<String, ElbVal>, key: &str) -> Option<i64> {
    m.get(key).and_then(ElbVal::opt_i64)
}

/// Borrow an `ElbVal::M` map, or `None` for other variants.
fn raw_map_ref(v: &ElbVal) -> Option<&BTreeMap<String, ElbVal>> {
    match v {
        ElbVal::M(m) => Some(m),
        _ => None,
    }
}

/// Mutably borrow an `ElbVal::M` map, or `None`.
fn raw_map_ref_mut(v: &mut ElbVal) -> Option<&mut BTreeMap<String, ElbVal>> {
    match v {
        ElbVal::M(m) => Some(m),
        _ => None,
    }
}

/// Total instance count to hold for a desired capacity, honoring moto's
/// mixed-instances weighted math (moto's set_desired_capacity).
fn target_instance_count(group: &Group, target_capacity: i64) -> i64 {
    let overrides: Vec<Params> = group
        .mixed_instances_policy
        .as_ref()
        .and_then(|p| p.get("LaunchTemplate"))
        .and_then(|lt| lt.get("Overrides"))
        .map(ElbVal::map_list)
        .unwrap_or_default();
    if overrides.is_empty() {
        return target_capacity.max(0);
    }
    let distribution: BTreeMap<String, ElbVal> = group
        .mixed_instances_policy
        .as_ref()
        .and_then(|p| p.get("InstancesDistribution"))
        .and_then(raw_map_ref)
        .cloned()
        .unwrap_or_default();
    let on_demand_base = elb_i64(&distribution, "OnDemandBaseCapacity").unwrap_or(0);
    let percent_above =
        elb_i64(&distribution, "OnDemandPercentageAboveBaseCapacity").unwrap_or(100);
    let weight = primary_weight_of(&overrides);
    if on_demand_base >= target_capacity {
        // Whole capacity is On-Demand (moto's branch), weight-scaled.
        return ceil_div(target_capacity.max(0), weight);
    }
    let above = target_capacity - on_demand_base;
    // moto: int(above * percent) / 100.0 (float truncation), then int sums.
    let on_demand_above = ((above as f64 * percent_above as f64) as i64) as f64 / 100.0;
    let spot_above = ((above as f64 - on_demand_above) as i64).max(0);
    let total_on_demand = on_demand_base + (on_demand_above as i64);
    ceil_div(total_on_demand, weight) + ceil_div(spot_above, weight)
}

/// Set the group's desired capacity, launching or terminating EC2 instances to
/// match. `None` desired means min_size (moto's default).
fn set_group_capacity(req: &Request, group: &mut Group, new_capacity: Option<i64>) {
    group.desired_capacity = new_capacity.unwrap_or(group.min_size.unwrap_or(0));
    let current = group.active_instances().len();
    let target_capacity = group.desired_capacity.max(0);
    let target = target_instance_count(group, target_capacity);

    match target as isize - current as isize {
        0 => {}
        n if n > 0 => {
            for inst in launch_instances(req, group, n as usize) {
                group.instance_states.push(inst);
            }
        }
        n => {
            let count_to_remove = (-n) as usize;
            let ids_to_remove: Vec<String> = group
                .instance_states
                .iter()
                .filter(|x| !x.protected_from_scale_in)
                .take(count_to_remove)
                .map(|x| x.instance_id.clone())
                .collect();
            if !ids_to_remove.is_empty() {
                group
                    .instance_states
                    .retain(|x| !ids_to_remove.contains(&x.instance_id));
                terminate_ec2(req, &ids_to_remove);
            }
        }
    }
    sync_attached_elbs(req, group);
    sync_target_groups(req, group);
}

/// Capacity delta for a policy execution (moto's FakeScalingPolicy.execute).
fn execute_policy_change(req: &Request, state: &mut AsgState, policy: &ScalingPolicy) {
    let asg_name = policy.auto_scaling_group_name.clone();
    let Some(group) = state.autoscaling_groups.get_mut(&asg_name) else {
        return;
    };
    match policy.adjustment_type.as_deref() {
        Some("ExactCapacity") => {
            let desired = policy.scaling_adjustment;
            set_group_capacity(req, group, desired);
        }
        Some("PercentChangeInCapacity") => {
            // moto: 0..1 rounds up to 1, else floor.
            let current = group.desired_capacity;
            let adjustment = policy.scaling_adjustment.unwrap_or(0);
            let scaled = current as f64 * (1.0 + (adjustment as f64) / 100.0);
            let desired = if (current as f64).lt(&scaled) && scaled < (current as f64) + 1.0 {
                current + 1
            } else {
                scaled as i64
            };
            set_group_capacity(req, group, Some(desired));
        }
        _ => {
            // ChangeInCapacity (and any unmodeled type): additive.
            let desired = group.desired_capacity + policy.scaling_adjustment.unwrap_or(0);
            set_group_capacity(req, group, Some(desired));
        }
    }
}

// ---------------------------------------------------------------------------
// Record construction from params (moto's backend methods).
// ---------------------------------------------------------------------------

/// Tag normalization the tags setter performs (moto's `tags.setter`).
fn normalize_group_tags(group_name: &str, tags: &[Params]) -> Vec<TagRecord> {
    tags.iter()
        .map(|t| TagRecord {
            resource_id: elb_string(t, "ResourceId")
                .filter(|v| !v.is_empty())
                .unwrap_or(group_name)
                .to_string(),
            resource_type: elb_string(t, "ResourceType")
                .filter(|v| !v.is_empty())
                .unwrap_or("auto-scaling-group")
                .to_string(),
            key: elb_string(t, "Key").unwrap_or_default().to_string(),
            value: elb_string(t, "Value").map(std::string::ToString::to_string),
            propagate: t
                .get("PropagateAtLaunch")
                .and_then(ElbVal::opt_bool)
                .unwrap_or(false),
        })
        .collect()
}

/// Resolve AZs and the VPC zone from the request (moto's
/// `_set_azs_and_vpcs`); `carried_vpc` keeps the group's existing identifier
/// on AZ-only updates, `update` skips the both-empty error.
fn resolve_azs_and_vpcs(
    req: &Request,
    availability_zones: &[String],
    vpc_zone_identifier: Option<&str>,
    update: bool,
    carried_vpc: Option<String>,
) -> Result<(Vec<String>, Option<String>), AsgError> {
    let vpc = vpc_zone_identifier
        .filter(|v| !v.is_empty())
        .map(|v| v.to_string())
        .or(carried_vpc);
    if let Some(vpc_id) = &vpc {
        let subnet_ids: Vec<String> = vpc_id.split(',').map(|s| s.trim().to_string()).collect();
        let vpc_zones = ec2_subnet_azs(req, &subnet_ids);
        if !availability_zones.is_empty()
            && sorted_ascii(availability_zones) != sorted_ascii(&vpc_zones)
        {
            return Err(validation(
                "The availability zones of the specified subnets and the Auto Scaling group do not match",
            ));
        }
        return Ok((
            vpc_zones, // moto adopts the subnets' AZs in this branch
            Some(vpc_id.clone()),
        ));
    }
    if availability_zones.is_empty() {
        if !update {
            return Err(validation(
                "At least one Availability Zone or VPC Subnet is required.",
            ));
        }
        return Ok((availability_zones.to_vec(), None));
    }
    Ok((availability_zones.to_vec(), None))
}

fn sorted_ascii(v: &[String]) -> Vec<String> {
    let mut out = v.to_vec();
    out.sort();
    out
}

/// az per subnet id; missing ids dropped like moto's describe_subnets probe.
fn ec2_subnet_azs(req: &Request, subnet_ids: &[String]) -> Vec<String> {
    let slot = ec2::state::shared();
    let mut map = slot.write();
    let st = map
        .entry((req.account, req.region.clone()))
        .or_insert_with(|| {
            let mut s = ec2::state::Ec2State::default();
            s.ensure_default_vpc(&req.region, req.account);
            s
        });
    st.subnets
        .iter()
        .filter(|s| subnet_ids.contains(&s.id))
        .map(|s| s.az.clone())
        .collect()
}

// ---------------------------------------------------------------------------
// Result renderers (botocore output-shape member order; None members omitted
// exactly like moto's shape-driven serializer skips absent attributes).
// ---------------------------------------------------------------------------

fn list_scaped(name: &str, values: &[String]) -> String {
    list_xml(name, &values.iter().map(|s| esc(s)).collect::<Vec<_>>())
}

/// moto's `block_device_mappings` property normalization: ephemeral VBDs keep
/// VirtualName, NoDevice=="true" carries NoDevice, the remaining members come
/// from the Ebs sub-struct (unset members omitted like moto's serializer).
fn bdm_xml(m: &Params) -> String {
    let device_name = elb_string(m, "DeviceName").unwrap_or_default();
    let virtual_name = elb_string(m, "VirtualName");
    let mut out = String::new();
    if let Some(vn) = virtual_name.filter(|v| v.contains("ephemeral")) {
        out.push_str(&tag_esc("VirtualName", vn));
    }
    out.push_str(&tag_esc("DeviceName", device_name));
    let ebs_src: BTreeMap<String, ElbVal> = m
        .get("Ebs")
        .and_then(raw_map_ref)
        .cloned()
        .unwrap_or_default();
    let mut ebs = String::new();
    for shape in [
        "SnapshotId",
        "VolumeSize",
        "VolumeType",
        "DeleteOnTermination",
        "Iops",
        "Encrypted",
        "Throughput",
    ] {
        if let Some(v) = elb_string(&ebs_src, shape) {
            ebs.push_str(&tag_esc(shape, v));
        }
    }
    out.push_str(&tag_raw("Ebs", &ebs));
    if elb_string(m, "NoDevice") == Some("true") {
        out.push_str(&tag_esc("NoDevice", "true"));
    }
    out
}

fn lc_xml(lc: &LaunchConfig) -> String {
    let mut out = String::new();
    out.push_str(&tag_esc("LaunchConfigurationName", &lc.name));
    out.push_str(&tag_esc("LaunchConfigurationARN", &lc.arn));
    out.push_str(&tag_esc("ImageId", &lc.image_id));
    if let Some(k) = &lc.key_name {
        out.push_str(&tag_esc("KeyName", k));
    }
    out.push_str(&list_scaped("SecurityGroups", &lc.security_groups));
    if let Some(v) = lc.classic_link_vpc_id.as_deref() {
        out.push_str(&tag_esc("ClassicLinkVPCId", v));
    }
    if !lc.classic_link_vpc_security_groups.is_empty() {
        out.push_str(&list_scaped(
            "ClassicLinkVPCSecurityGroups",
            &lc.classic_link_vpc_security_groups,
        ));
    }
    if let Some(u) = lc.user_data.as_deref() {
        out.push_str(&tag_esc("UserData", u));
    }
    out.push_str(&tag_esc("InstanceType", &lc.instance_type));
    if let Some(k) = lc.kernel_id.as_deref() {
        out.push_str(&tag_esc("KernelId", k));
    }
    if let Some(r) = lc.ramdisk_id.as_deref() {
        out.push_str(&tag_esc("RamdiskId", r));
    }
    if !lc.block_device_mappings.is_empty() {
        let members: Vec<String> = lc.block_device_mappings.iter().map(bdm_xml).collect();
        out.push_str(&list_xml("BlockDeviceMappings", &members));
    }
    out.push_str(&tag_raw(
        "InstanceMonitoring",
        &tag_esc("Enabled", bool_text(lc.instance_monitoring_enabled)),
    ));
    if let Some(s) = lc.iam_instance_profile.as_deref() {
        out.push_str(&tag_esc("IamInstanceProfile", s));
    }
    if let Some(s) = lc.spot_price.as_deref() {
        out.push_str(&tag_esc("SpotPrice", s));
    }
    if let Some(mo) = &lc.metadata_options {
        out.push_str(&tag_raw("MetadataOptions", &param_members(mo)));
    }
    out.push_str(&tag_esc("CreatedTime", &lc.created_time));
    out.push_str(&tag_esc("EbsOptimized", bool_text(lc.ebs_optimized)));
    out.push_str(&tag_esc(
        "AssociatePublicIpAddress",
        bool_text(lc.associate_public_ip_address),
    ));
    out
}

/// `<SuspendedProcesses>` entries (moto's `{ProcessName, SuspensionReason}`).
fn suspended_xml(group: &Group) -> String {
    let members: Vec<String> = group
        .suspended_processes
        .iter()
        .map(|p| {
            format!(
                "{}{}",
                tag_esc("ProcessName", p),
                tag_esc("SuspensionReason", "")
            )
        })
        .collect();
    list_xml("SuspendedProcesses", &members)
}

fn enabled_metrics_xml(group: &Group) -> String {
    let members: Vec<String> = group
        .metrics
        .iter()
        .map(|m| {
            format!(
                "{}{}",
                tag_esc("Metric", m),
                tag_esc("Granularity", "1Minute")
            )
        })
        .collect();
    list_xml("EnabledMetrics", &members)
}

fn tags_xml(group: &Group) -> String {
    let members: Vec<String> = group
        .tags
        .iter()
        .map(|t| {
            format!(
                "{}{}{}{}{}",
                tag_esc("ResourceId", &t.resource_id),
                tag_esc("ResourceType", &t.resource_type),
                tag_esc("Key", &t.key),
                tag_opt("Value", t.value.as_deref()),
                tag_esc("PropagateAtLaunch", bool_text(t.propagate)),
            )
        })
        .collect();
    list_xml("Tags", &members)
}

fn traffic_sources_xml(group: &Group) -> String {
    let members: Vec<String> = group
        .traffic_sources
        .iter()
        .map(|ts| param_members(&only_members(ts, &["Identifier", "Type"])))
        .collect();
    list_xml("TrafficSources", &members)
}

/// Restrict a stored struct to a fixed member list (drop parsing noise).
fn only_members(m: &Params, keys: &[&str]) -> BTreeMap<String, ElbVal> {
    let mut out = BTreeMap::new();
    for k in keys {
        if let Some(v) = m.get(*k) {
            if !matches!(v, ElbVal::None) {
                out.insert((*k).to_string(), v.clone());
            }
        }
    }
    out
}

/// WarmPoolConfiguration member body.
fn warm_pool_xml(wp: &WarmPool) -> String {
    let mut out = String::new();
    if let Some(v) = &wp.max_group_prepared_capacity {
        out.push_str(&tag_esc("MaxGroupPreparedCapacity", &int_str(v)));
    }
    out.push_str(&tag_esc("MinSize", &int_str(&wp.min_size)));
    out.push_str(&tag_esc("PoolState", &wp.pool_state));
    if let Some(reuse) = &wp.instance_reuse_policy {
        out.push_str(&tag_raw(
            "InstanceReusePolicy",
            &param_members(&only_members(reuse, &["ReuseOnScaleIn"])),
        ));
    }
    out
}

/// InstanceRefresh member body (fork's InstanceRefresh attributes).
fn refresh_xml(r: &InstanceRefresh) -> String {
    let mut out = String::new();
    out.push_str(&tag_esc("InstanceRefreshId", &r.instance_refresh_id));
    out.push_str(&tag_esc("AutoScalingGroupName", &r.auto_scaling_group_name));
    out.push_str(&tag_esc("Status", &r.status));
    out.push_str(&tag_esc("StartTime", &r.start_time));
    if let Some(e) = r.end_time.as_deref() {
        out.push_str(&tag_esc("EndTime", e));
    }
    out.push_str(&tag_esc("PercentageComplete", "0"));
    out.push_str(&tag_esc("InstancesToUpdate", "0"));
    if let Some(prefs) = &r.preferences {
        out.push_str(&tag_raw("Preferences", &param_members(prefs)));
    }
    out
}

/// LifecycleHook member body (moto hardcodes the template-only rows).
fn hook_xml(h: &LifecycleHook) -> String {
    let mut out = String::new();
    out.push_str(&tag_esc("LifecycleHookName", &h.name));
    out.push_str(&tag_esc("AutoScalingGroupName", &h.auto_scaling_group_name));
    if let Some(t) = h.lifecycle_transition.as_deref() {
        out.push_str(&tag_esc("LifecycleTransition", t));
    }
    out.push_str(&tag_esc(
        "NotificationTargetARN",
        "arn:aws:sqs:us-east-1:123456789012:my-queue",
    ));
    out.push_str(&tag_esc(
        "RoleARN",
        "arn:aws:iam::1234567890:role/my-auto-scaling-role",
    ));
    out.push_str(&tag_esc(
        "HeartbeatTimeout",
        &h.heartbeat_timeout.to_string(),
    ));
    out.push_str(&tag_esc("GlobalTimeout", "172800"));
    out.push_str(&tag_esc("DefaultResult", &h.default_result));
    out
}

/// ScheduledUpdateGroupAction member body.
fn scheduled_xml(sa: &ScheduledAction) -> String {
    let mut out = String::new();
    out.push_str(&tag_esc(
        "AutoScalingGroupName",
        &sa.auto_scaling_group_name,
    ));
    out.push_str(&tag_esc("ScheduledActionName", &sa.scheduled_action_name));
    if let Some(v) = sa.start_time.as_deref() {
        out.push_str(&tag_esc("StartTime", v));
    }
    if let Some(v) = sa.end_time.as_deref() {
        out.push_str(&tag_esc("EndTime", v));
    }
    if let Some(v) = sa.recurrence.as_deref() {
        out.push_str(&tag_esc("Recurrence", v));
    }
    if let Some(v) = sa.min_size {
        out.push_str(&tag_esc("MinSize", &v.to_string()));
    }
    if let Some(v) = sa.max_size {
        out.push_str(&tag_esc("MaxSize", &v.to_string()));
    }
    if let Some(v) = sa.desired_capacity {
        out.push_str(&tag_esc("DesiredCapacity", &v.to_string()));
    }
    if let Some(v) = sa.time_zone.as_deref() {
        out.push_str(&tag_esc("TimeZone", v));
    }
    out
}

/// ScalingPolicy member body.
fn policy_xml(p: &ScalingPolicy, region: &str, account: u64) -> String {
    let mut out = String::new();
    out.push_str(&tag_esc("AutoScalingGroupName", &p.auto_scaling_group_name));
    out.push_str(&tag_esc("PolicyName", &p.name));
    out.push_str(&tag_esc("PolicyARN", &policy_arn(p, region, account)));
    out.push_str(&tag_esc("PolicyType", &p.policy_type));
    if let Some(v) = p.adjustment_type.as_deref() {
        out.push_str(&tag_esc("AdjustmentType", v));
    }
    if let Some(v) = p.min_adjustment_magnitude.as_deref() {
        out.push_str(&tag_esc("MinAdjustmentMagnitude", &int_str(v)));
    }
    if let Some(v) = p.scaling_adjustment {
        out.push_str(&tag_esc("ScalingAdjustment", &v.to_string()));
    }
    if let Some(v) = p.cooldown {
        out.push_str(&tag_esc("Cooldown", &v.to_string()));
    }
    if !p.step_adjustments.is_empty() {
        let members: Vec<String> = p.step_adjustments.iter().map(step_adjustment_xml).collect();
        out.push_str(&list_xml("StepAdjustments", &members));
    }
    if let Some(v) = p.metric_aggregation_type.as_deref() {
        out.push_str(&tag_esc("MetricAggregationType", v));
    }
    if let Some(v) = p.estimated_instance_warmup.as_deref() {
        out.push_str(&tag_esc("EstimatedInstanceWarmup", &int_str(v)));
    }
    if let Some(ttc) = &p.target_tracking_config {
        out.push_str(&tag_raw(
            "TargetTrackingConfiguration",
            &target_tracking_xml(ttc),
        ));
    }
    if let Some(psc) = &p.predictive_scaling_configuration {
        out.push_str(&tag_raw(
            "PredictiveScalingConfiguration",
            &param_members(psc),
        ));
    }
    out
}

/// moto's policy ARN: a fixed uuid per region/account with the ASG + policy
/// names embedded (captured verbatim from the fork).
fn policy_arn(p: &ScalingPolicy, region: &str, account: u64) -> String {
    format!(
        "arn:{}:autoscaling:{}:{}:scalingPolicy:{}:autoScalingGroupName/{}:policyName/{}",
        partition(region),
        region,
        account,
        POLICY_ARN_UUID,
        p.auto_scaling_group_name,
        p.name
    )
}

fn step_adjustment_xml(m: &Params) -> String {
    let mut out = String::new();
    if let Some(v) = elb_string(m, "MetricIntervalLowerBound") {
        out.push_str(&tag_esc("MetricIntervalLowerBound", &float_str(v)));
    }
    if let Some(v) = elb_string(m, "MetricIntervalUpperBound") {
        out.push_str(&tag_esc("MetricIntervalUpperBound", &float_str(v)));
    }
    if let Some(v) = elb_string(m, "ScalingAdjustment") {
        out.push_str(&tag_esc("ScalingAdjustment", &int_str(v)));
    }
    out
}

/// TargetTrackingConfiguration body, with moto's TargetTrackingConfiguration
/// __init__ defaults (Dimensions [] under CustomizedMetricSpecification,
/// ReturnData True on unqualified Metrics entries).
fn target_tracking_xml(ttc: &Params) -> String {
    let mut out = String::new();
    if let Some(pre) = ttc.get("PredefinedMetricSpecification") {
        out.push_str(&tag_raw(
            "PredefinedMetricSpecification",
            &param_member_flat(pre, &["PredefinedMetricType", "ResourceLabel"]),
        ));
    }
    if let Some(cust) = ttc.get("CustomizedMetricSpecification") {
        out.push_str(&tag_raw(
            "CustomizedMetricSpecification",
            &custom_metric_xml(cust),
        ));
    }
    if let Some(v) = elb_string(ttc, "TargetValue") {
        out.push_str(&tag_esc("TargetValue", &float_str(v)));
    }
    if let Some(v) = elb_string(ttc, "DisableScaleIn") {
        out.push_str(&tag_esc("DisableScaleIn", v));
    }
    out
}

/// Scalar-only passthrough of a subset of members.
fn param_member_flat(v: &ElbVal, keys: &[&str]) -> String {
    let Some(m) = raw_map_ref(v) else {
        return String::new();
    };
    let mut out = String::new();
    for k in keys {
        if let Some(val) = elb_string(m, k) {
            out.push_str(&tag_esc(k, val));
        }
    }
    out
}

/// CustomizedMetricSpecification with moto's Dimensions default.
fn custom_metric_xml(cust: &ElbVal) -> String {
    let Some(raw) = raw_map_ref(cust) else {
        return String::new();
    };
    let mut out = String::new();
    for k in ["MetricName", "Namespace", "Statistic", "Unit"] {
        if let Some(v) = elb_string(raw, k) {
            out.push_str(&tag_esc(k, v));
        }
    }
    if let Some(dims) = raw.get("Dimensions") {
        let list: Vec<String> = dims
            .map_list()
            .iter()
            .map(|d| {
                format!(
                    "{}{}",
                    tag_esc("Name", elb_string(d, "Name").unwrap_or_default()),
                    tag_esc("Value", elb_string(d, "Value").unwrap_or_default()),
                )
            })
            .collect();
        out.push_str(&list_xml("Dimensions", &list));
    }
    if let Some(v) = elb_string(raw, "Period") {
        out.push_str(&tag_esc("Period", &int_str(v)));
    }
    if let Some(metrics) = raw.get("Metrics") {
        let list: Vec<String> = metrics.map_list().iter().map(metric_query_xml).collect();
        out.push_str(&list_xml("Metrics", &list));
    }
    out
}

/// TargetTrackingMetricDataQuery body with the ReturnData default.
fn metric_query_xml(q: &BTreeMap<String, ElbVal>) -> String {
    let mut out = String::new();
    for k in ["Id", "Expression", "Label", "Period"] {
        if let Some(v) = elb_string(q, k) {
            if k == "Period" {
                out.push_str(&tag_esc("Period", &int_str(v)));
            } else {
                out.push_str(&tag_esc(k, v));
            }
        }
    }
    if let Some(stat) = q.get("MetricStat") {
        let Some(stat_map) = raw_map_ref(stat) else {
            return out;
        };
        let mut inner = String::new();
        if let Some(metric) = stat_map.get("Metric") {
            let Some(m) = raw_map_ref(metric) else {
                return out;
            };
            let mut metric_xml = String::new();
            for k in ["Namespace", "MetricName"] {
                if let Some(v) = elb_string(m, k) {
                    metric_xml.push_str(&tag_esc(k, v));
                }
            }
            if let Some(dims) = m.get("Dimensions") {
                let list: Vec<String> = dims
                    .map_list()
                    .iter()
                    .map(|d| {
                        format!(
                            "{}{}",
                            tag_esc("Name", elb_string(d, "Name").unwrap_or_default()),
                            tag_esc("Value", elb_string(d, "Value").unwrap_or_default()),
                        )
                    })
                    .collect();
                metric_xml.push_str(&list_xml("Dimensions", &list));
            }
            inner.push_str(&tag_raw("Metric", &metric_xml));
        }
        for k in ["Stat", "Unit"] {
            if let Some(v) = elb_string(stat_map, k) {
                out_push_key(&mut inner, k, v);
            }
        }
        if let Some(v) = elb_i64(stat_map, "Period") {
            inner.push_str(&tag_esc("Period", &v.to_string()));
        }
        out.push_str(&tag_raw("MetricStat", &inner));
    }
    if let Some(rd) = elb_string(q, "ReturnData") {
        out.push_str(&tag_esc("ReturnData", rd));
    } else {
        out.push_str(&tag_esc("ReturnData", "true"));
    }
    out
}

fn out_push_key(out: &mut String, key: &str, value: &str) {
    out.push_str(&tag_esc(key, value));
}

// ---------------------------------------------------------------------------
// Operation handlers (moto's `AutoScalingResponse.*` + backend methods).
// ---------------------------------------------------------------------------

fn op_create_launch_configuration(
    state: &mut AsgState,
    p: &Params,
    account: u64,
    region: &str,
) -> OpResult<String> {
    let name = p_string(p, "LaunchConfigurationName")
        .unwrap_or_default()
        .to_string();
    let instance_id_param = p_string(p, "InstanceId");
    let image_id = p_string(p, "ImageId");
    let instance_type = p_string(p, "InstanceType");
    // moto: one of InstanceId, or both ImageId and InstanceType.
    if instance_id_param.is_none() && (image_id.is_none() || instance_type.is_none()) {
        return Err(validation(
            "Valid requests must contain either the InstanceID parameter or both the ImageId and InstanceType parameters.",
        ));
    }
    let monitoring_enabled = p
        .get("InstanceMonitoring")
        .and_then(raw_map_ref)
        .and_then(|m| m.get("Enabled"))
        .and_then(ElbVal::opt_bool)
        .unwrap_or(false);
    let lc = LaunchConfig {
        name: name.clone(),
        image_id: image_id.unwrap_or_default().to_string(),
        key_name: p_string(p, "KeyName").map(std::string::ToString::to_string),
        ramdisk_id: p_string(p, "RamdiskId").map(std::string::ToString::to_string),
        kernel_id: p_string(p, "KernelId").map(std::string::ToString::to_string),
        security_groups: p_list(p, "SecurityGroups"),
        user_data: p_string(p, "UserData").map(std::string::ToString::to_string),
        instance_type: instance_type.unwrap_or_default().to_string(),
        instance_monitoring_enabled: monitoring_enabled,
        iam_instance_profile: p_string(p, "IamInstanceProfile")
            .map(std::string::ToString::to_string),
        spot_price: p_string(p, "SpotPrice").map(std::string::ToString::to_string),
        ebs_optimized: p_bool(p, "EbsOptimized").unwrap_or(false),
        associate_public_ip_address: p_bool(p, "AssociatePublicIpAddress").unwrap_or(false),
        block_device_mappings: p_struct_list(p, "BlockDeviceMappings"),
        metadata_options: p.get("MetadataOptions").and_then(raw_map_ref).cloned(),
        classic_link_vpc_id: p_string(p, "ClassicLinkVPCId").map(std::string::ToString::to_string),
        classic_link_vpc_security_groups: p_list(p, "ClassicLinkVPCSecurityGroups"),
        arn: format!(
            "arn:{}:autoscaling:{}:{}:launchConfiguration:{}:launchConfigurationName/{}",
            partition(region),
            region,
            account,
            LC_ARN_UUID,
            name,
        ),
        created_time: now_wire_ts(),
    };
    // moto overwrites silently on a duplicate name.
    state.launch_configurations.insert(lc.name.clone(), lc);
    Ok(String::new())
}

fn op_describe_launch_configurations(state: &AsgState, p: &Params) -> OpResult<String> {
    let names: Vec<String> = p_list(p, "LaunchConfigurationNames");
    let all: Vec<&LaunchConfig> = state
        .launch_configurations
        .values()
        .filter(|c| names.is_empty() || names.iter().any(|n| n == &c.name))
        .collect();
    let marker = p_string(p, "NextToken").unwrap_or_default();
    let mut start = 0usize;
    if !marker.is_empty() {
        if let Some(pos) = all.iter().position(|c| c.name == marker) {
            start = pos + 1;
        }
    }
    let max_records = p_i64(p, "MaxRecords").unwrap_or(50).max(1) as usize;
    let page: Vec<&LaunchConfig> = all.iter().skip(start).take(max_records).copied().collect();
    let mut body = list_xml(
        "LaunchConfigurations",
        &page.iter().map(|c| lc_xml(c)).collect::<Vec<_>>(),
    );
    if all.len() > start + max_records {
        if let Some(last) = page.last() {
            body.push_str(&tag_esc("NextToken", &last.name));
        }
    }
    Ok(body)
}

fn op_delete_launch_configuration(state: &mut AsgState, p: &Params) -> OpResult<String> {
    let name = p_string(p, "LaunchConfigurationName").unwrap_or_default();
    state.launch_configurations.remove(name);
    Ok(String::new())
}

/// moto's `TargetTrackingConfiguration`-style nested-launch resolution:
/// `(template_id, template_name, provided_version)`.
fn resolve_lt_spec(spec: &Params) -> OpResult<(Option<String>, Option<String>, Option<String>)> {
    let template_id = elb_string(spec, "LaunchTemplateId").unwrap_or_default();
    let template_name = elb_string(spec, "LaunchTemplateName").unwrap_or_default();
    if template_id.is_empty() == template_name.is_empty() {
        return Err(validation(
            "Valid requests must contain either launchTemplateId or LaunchTemplateName",
        ));
    }
    let version = elb_string(spec, "Version")
        .filter(|v| !v.is_empty())
        .map(std::string::ToString::to_string);
    Ok((
        (!template_id.is_empty()).then(|| template_id.to_string()),
        (!template_name.is_empty()).then(|| template_name.to_string()),
        version,
    ))
}

/// Read launch-template version data (moto's `get_version` walk).
fn ec2_lt_version_data(
    req: &Request,
    template_id: Option<String>,
    template_name: Option<String>,
    version: &str,
) -> Option<(String, String, Option<String>, Option<String>)> {
    let slot = ec2::state::shared();
    let mut map = slot.write();
    let st = map
        .entry((req.account, req.region.clone()))
        .or_insert_with(|| {
            let mut s = ec2::state::Ec2State::default();
            s.ensure_default_vpc(&req.region, req.account);
            s
        });
    let pos = match (&template_id, &template_name) {
        (Some(id), _) if !id.is_empty() => st.launch_templates.iter().position(|l| &l.id == id)?,
        (None, Some(name)) if !name.is_empty() => {
            st.launch_templates.iter().position(|l| l.name == *name)?
        }
        _ => return None,
    };
    let lt = &st.launch_templates[pos];
    let number = if version == "$Latest" {
        lt.latest_version
    } else if version == "$Default" {
        lt.default_version
    } else {
        version.parse().unwrap_or(lt.default_version)
    };
    let data = lt
        .versions
        .iter()
        .find(|v| v.number == number)
        .map(|v| v.data.clone())?;
    let obj = serde_json::from_str::<serde_json::Value>(&data).ok()?;
    let get = |k: &str| {
        obj.get(k)
            .and_then(|v| v.as_str())
            .map(std::string::ToString::to_string)
    };
    Some((
        get("imageId")?,
        get("instanceType").unwrap_or_else(|| "m1.small".into()),
        get("keyName"),
        get("userData"),
    ))
}

#[allow(clippy::too_many_arguments)]
fn op_create_auto_scaling_group(
    req: &Request,
    state: &mut AsgState,
    p: &Params,
    account: u64,
    region: &str,
) -> OpResult<String> {
    let name = p_string(p, "AutoScalingGroupName")
        .unwrap_or_default()
        .to_string();
    let availability_zones: Vec<String> = p_list(p, "AvailabilityZones");
    let max_size = p_i64(p, "MaxSize");
    let min_size = p_i64(p, "MinSize");
    let desired_capacity = p_i64(p, "DesiredCapacity");
    let instance_id_param = p_string(p, "InstanceId").map(std::string::ToString::to_string);
    let mut launch_config_name =
        p_string(p, "LaunchConfigurationName").map(std::string::ToString::to_string);
    let launch_template_param = p
        .get("LaunchTemplate")
        .and_then(raw_map_ref)
        .filter(|m| !m.is_empty())
        .cloned();
    let mixed_instances_policy = p
        .get("MixedInstancesPolicy")
        .and_then(raw_map_ref)
        .filter(|m| !m.is_empty())
        .cloned();

    // exactly one launch-config-like parameter (moto's backend check)
    provided_count_check(
        launch_config_name.is_some(),
        launch_template_param.is_some(),
        instance_id_param.is_some(),
        mixed_instances_policy.is_some(),
    )?;

    let (availability_zones, vpc_zone_identifier) = resolve_azs_and_vpcs(
        req,
        &availability_zones,
        p_string(p, "VPCZoneIdentifier"),
        false,
        None,
    )?;

    if let Some(instance_id) = instance_id_param.clone() {
        let view =
            ec2_get_instance(req, &instance_id).ok_or_else(|| invalid_instance(&instance_id))?;
        let lc_name = name.clone();
        let created = LaunchConfig {
            name: lc_name.clone(),
            image_id: view.image_id,
            key_name: view.key_name,
            ramdisk_id: None,
            kernel_id: None,
            security_groups: view.security_group_names,
            user_data: view.user_data.clone(),
            instance_type: view.instance_type,
            instance_monitoring_enabled: false,
            iam_instance_profile: None,
            spot_price: None,
            ebs_optimized: false,
            associate_public_ip_address: false,
            block_device_mappings: Vec::new(),
            metadata_options: None,
            classic_link_vpc_id: None,
            classic_link_vpc_security_groups: Vec::new(),
            arn: format!(
                "arn:{}:autoscaling:{}:{}:launchConfiguration:{}:launchConfigurationName/{}",
                partition(region),
                region,
                account,
                LC_ARN_UUID,
                lc_name,
            ),
            created_time: now_wire_ts(),
        };
        state.launch_configurations.insert(lc_name.clone(), created);
        launch_config_name = Some(lc_name);
    }

    // Resolve the single launch-config-like param (moto's
    // `_set_launch_configuration`).
    let lt_spec = if let Some(template) = launch_template_param.as_ref() {
        resolve_lt_spec(template)?
    } else if let Some(mip) = mixed_instances_policy.as_ref() {
        match mip
            .get("LaunchTemplate")
            .and_then(|lt| lt.get("LaunchTemplateSpecification"))
            .and_then(raw_map_ref)
        {
            Some(spec) => resolve_lt_spec(spec)?,
            None => (None, None, None),
        }
    } else {
        (None, None, None)
    };
    let (lt_id, lt_name, provided_version) = lt_spec;
    let mut launch_template_id = lt_id;
    let mut launch_template_name_opt = lt_name;

    if launch_template_id.is_some() || launch_template_name_opt.is_some() {
        let Some((tpl_id, tpl_name, _)) = ec2_launch_template(
            req,
            launch_template_id.as_deref().unwrap_or_default(),
            launch_template_name_opt.as_deref().unwrap_or_default(),
        ) else {
            return Err(AsgError::new(
                400,
                "InvalidLaunchTemplateName.NotFoundException",
                "The specified launch template, if it exists, could not be located",
            ));
        };
        launch_template_id = Some(tpl_id.clone());
        launch_template_name_opt = Some(tpl_name);
    } else {
        let Some(lc_name) = launch_config_name.clone() else {
            return Err(validation(format!(
                "AutoScalingGroup name not found - Unable to load launch config for {name}"
            )));
        };
        if !state.launch_configurations.contains_key(&lc_name) {
            return Err(validation(format!(
                "Unable to load launch configuration '{lc_name}'."
            )));
        }
    }

    // mixed_instances_policy normalization (moto's constructor defaults)
    let health_check_period = p_i64(p, "HealthCheckGracePeriod");
    let placement_group = p_string(p, "PlacementGroup").map(std::string::ToString::to_string);
    let termination_policies = p_list(p, "TerminationPolicies");
    let tags = p_struct_list(p, "Tags");
    let mut mip = mixed_instances_policy.clone();
    if let Some(m) = mip.as_mut() {
        if let Some(dist) = m.get_mut("InstancesDistribution") {
            if matches!(dist, ElbVal::None) {
                *dist = ElbVal::M(BTreeMap::new());
            }
        } else {
            m.insert(
                "InstancesDistribution".to_string(),
                ElbVal::M(BTreeMap::new()),
            );
        }
        if let Some(lt) = m.get_mut("LaunchTemplate") {
            if let Some(lt_map) = raw_map_ref_mut(lt) {
                if let Some(spec) = lt_map.get_mut("LaunchTemplateSpecification") {
                    if let Some(spec_map) = raw_map_ref_mut(spec) {
                        if let Some(id) = launch_template_id.clone() {
                            spec_map.insert("LaunchTemplateId".into(), ElbVal::S(id));
                        }
                        if let Some(n) = launch_template_name_opt.clone() {
                            spec_map.insert("LaunchTemplateName".into(), ElbVal::S(n));
                        }
                        spec_map.insert(
                            "Version".into(),
                            ElbVal::S(
                                provided_version
                                    .clone()
                                    .unwrap_or_else(|| "$Default".to_string()),
                            ),
                        );
                    }
                }
                if lt_map.get("Overrides").is_none() {
                    lt_map.insert("Overrides".to_string(), ElbVal::L(Vec::new()));
                }
            }
        }
    }

    let lc_snapshot = launch_config_name
        .as_ref()
        .and_then(|n| state.launch_configurations.get(n))
        .map(|lc| {
            (
                Some(lc.image_id.clone()),
                Some(lc.instance_type.clone()),
                lc.key_name.clone(),
                lc.user_data.clone(),
            )
        });
    let snapshot = match (&launch_template_id, &launch_template_name_opt) {
        (Some(tpl_id), tpl_name) if lc_snapshot.is_none() => {
            let version = provided_version
                .clone()
                .unwrap_or_else(|| "$Default".to_string());
            ec2_lt_version_data(
                req,
                Some(tpl_id.clone()),
                Some(tpl_name.clone().unwrap_or_default()),
                &version,
            )
            .map(|(image, itype, key, user)| (Some(image), Some(itype), key, user))
        }
        _ => None,
    };
    let (launch_image_id, launch_instance_type, launch_key_name, launch_user_data) =
        lc_snapshot.or(snapshot).unwrap_or((None, None, None, None));
    let ct_launch_template_id = launch_template_id.clone();
    let group = Group {
        name: name.clone(),
        id: uuid::Uuid::new_v4().to_string(),
        region: req.region.clone(),
        account_id: req.account,
        availability_zones: availability_zones.clone(),
        vpc_zone_identifier: vpc_zone_identifier.clone(),
        max_size,
        min_size,
        desired_capacity: 0,
        mixed_instances_policy: mip,
        launch_config_name: launch_config_name.clone(),
        launch_template_id,
        launch_template_name: launch_template_name_opt,
        provided_launch_template_version: provided_version.clone(),
        launch_image_id,
        launch_instance_type,
        launch_key_name,
        launch_user_data,
        default_cooldown: default_cooldown_freeze(p),
        health_check_grace_period: Some(health_check_period.unwrap_or(300)),
        health_check_type: health_check_type_fallback(p),
        load_balancer_names: p_list(p, "LoadBalancerNames"),
        target_group_arns: p_list(p, "TargetGroupARNs"),
        placement_group,
        termination_policies: if termination_policies.is_empty() {
            vec!["Default".to_string()]
        } else {
            termination_policies.clone()
        },
        capacity_rebalance: p_bool(p, "CapacityRebalance").unwrap_or(false),
        new_instances_protected_from_scale_in: p_bool(p, "NewInstancesProtectedFromScaleIn")
            .unwrap_or(false),
        suspended_processes: Vec::new(),
        instance_states: Vec::new(),
        tags: normalize_group_tags(&name, &tags),
        metrics: Vec::new(),
        warm_pool: None,
        traffic_sources: Vec::new(),
        created_time: now_wire_ts(),
    };
    state.autoscaling_groups.insert(name.clone(), group);
    if let Some(group) = state.autoscaling_groups.get_mut(&name) {
        let desired_final = desired_capacity.or(min_size);
        set_group_capacity(req, group, desired_final);
    }
    // Successful mutate → CloudTrail management event on the default bus
    // (issue #347 allowlist). Fields the call carried, in the lower-camel
    // member shape real CloudTrail records use.
    emit_asg_cloudtrail(
        req,
        "CreateAutoScalingGroup",
        [
            ("autoScalingGroupName", Some(serde_json::json!(name))),
            ("minSize", min_size.map(|v| serde_json::json!(v))),
            ("maxSize", max_size.map(|v| serde_json::json!(v))),
            (
                "desiredCapacity",
                desired_capacity.map(|v| serde_json::json!(v)),
            ),
            (
                "availabilityZones",
                (!availability_zones.is_empty()).then(|| serde_json::json!(availability_zones)),
            ),
            (
                "launchConfigurationName",
                launch_config_name.as_ref().map(|v| serde_json::json!(v)),
            ),
            (
                "launchTemplateId",
                ct_launch_template_id.as_ref().map(|v| serde_json::json!(v)),
            ),
        ],
    );
    Ok(String::new())
}

fn provided_count_check(a: bool, b: bool, c: bool, d: bool) -> OpResult<()> {
    if usize::from(a) + usize::from(b) + usize::from(c) + usize::from(d) != 1 {
        return Err(validation(
            "Valid requests must contain either LaunchTemplate, LaunchConfigurationName, InstanceId or MixedInstancesPolicy parameter.",
        ));
    }
    Ok(())
}

fn default_cooldown_freeze(p: &Params) -> i64 {
    p_i64(p, "DefaultCooldown").unwrap_or(DEFAULT_COOLDOWN)
}

fn health_check_type_fallback(p: &Params) -> String {
    p_string(p, "HealthCheckType").unwrap_or("EC2").to_string()
}

// ---------------------------------------------------------------------------
// Group lifecycle ops (create lives with the snapshot helpers above).
// ---------------------------------------------------------------------------

/// UpdateAutoScalingGroup (moto's `update_auto_scaling_group` — DefaultCooldown,
/// PlacementGroup and TerminationPolicies are not updated, like the fork).
fn op_update_auto_scaling_group(
    req: &Request,
    state: &mut AsgState,
    p: &Params,
) -> OpResult<String> {
    let name = p_string(p, "AutoScalingGroupName")
        .unwrap_or_default()
        .to_string();
    let availability_zones: Vec<String> = p_list(p, "AvailabilityZones");
    let desired_capacity = p_i64(p, "DesiredCapacity");
    let max_size = p_i64(p, "MaxSize");
    let min_size = p_i64(p, "MinSize");
    let launch_config_name =
        p_string(p, "LaunchConfigurationName").map(std::string::ToString::to_string);
    let launch_template_param = p
        .get("LaunchTemplate")
        .and_then(raw_map_ref)
        .filter(|m| !m.is_empty())
        .cloned();
    let health_check_period = p_i64(p, "HealthCheckGracePeriod");
    let protected_flag = p_bool(p, "NewInstancesProtectedFromScaleIn");
    let mixed_instances_policy = p
        .get("MixedInstancesPolicy")
        .and_then(raw_map_ref)
        .filter(|m| !m.is_empty())
        .cloned();

    if launch_config_name.is_some() && launch_template_param.is_some() {
        return Err(validation(
            "Valid requests must contain either LaunchTemplate, LaunchConfigurationName or MixedInstancesPolicy parameter.",
        ));
    }
    if !state.autoscaling_groups.contains_key(&name) {
        return Err(validation("AutoScalingGroup name not found - null"));
    }
    let carried_vpc = state
        .autoscaling_groups
        .get(&name)
        .and_then(|g| g.vpc_zone_identifier.clone());
    let (resolved_azs, resolved_vpc) = resolve_azs_and_vpcs(
        req,
        &availability_zones,
        p_string(p, "VPCZoneIdentifier"),
        true,
        carried_vpc,
    )?;

    let mut desired = desired_capacity;
    if let Some(group) = state.autoscaling_groups.get_mut(&name) {
        if !resolved_azs.is_empty() {
            group.availability_zones = resolved_azs;
        }
        if resolved_vpc.is_some() {
            group.vpc_zone_identifier = resolved_vpc;
        }
        if max_size.is_some() {
            group.max_size = max_size;
        }
        if min_size.is_some() {
            group.min_size = min_size;
        }
        if desired.is_none() {
            let count = group.instance_states.len();
            if let Some(mn) = min_size {
                if mn > count as i64 {
                    desired = min_size;
                }
            }
            if desired.is_none() {
                if let Some(mx) = max_size {
                    if mx < count as i64 {
                        desired = max_size;
                    }
                }
            }
        }
        group.mixed_instances_policy = mixed_instances_policy;
        if let Some(lc_name) = launch_config_name.clone() {
            if !state.launch_configurations.contains_key(&lc_name) {
                return Err(validation(format!(
                    "Unable to load launch configuration '{lc_name}'."
                )));
            }
            group.launch_config_name = Some(lc_name);
            group.launch_template_id = None;
            group.launch_template_name = None;
            group.provided_launch_template_version = None;
        } else if let Some(template) = launch_template_param.as_ref() {
            let (lt_id, lt_name, provided_version) = resolve_lt_spec(template)?;
            let Some((tpl_id, tpl_name, _)) = ec2_launch_template(
                req,
                lt_id.as_deref().unwrap_or_default(),
                lt_name.as_deref().unwrap_or_default(),
            ) else {
                return Err(AsgError::new(
                    400,
                    "InvalidLaunchTemplateName.NotFoundException",
                    "The specified launch template, if it exists, could not be located",
                ));
            };
            group.launch_template_id = Some(tpl_id);
            group.launch_template_name = Some(tpl_name);
            group.provided_launch_template_version = provided_version;
        }
        if let Some(hp) = health_check_period {
            group.health_check_grace_period = Some(hp);
        }
        if let Some(hc) = p_string(p, "HealthCheckType") {
            group.health_check_type = hc.to_string();
        }
        if let Some(flag) = protected_flag {
            group.new_instances_protected_from_scale_in = flag;
        }
        // refresh the launch surface snapshot
        if let Some(config_name) = group.launch_config_name.clone() {
            let Some(lc) = state.launch_configurations.get(&config_name) else {
                return Err(validation(format!(
                    "Unable to load launch configuration '{config_name}'."
                )));
            };
            group.launch_image_id = Some(lc.image_id.clone());
            group.launch_instance_type = Some(lc.instance_type.clone());
            group.launch_key_name = lc.key_name.clone();
            group.launch_user_data = lc.user_data.clone();
        } else if let (Some(tpl_id), Some(tpl_name)) = (
            group.launch_template_id.clone(),
            group.launch_template_name.clone(),
        ) {
            let version = group
                .provided_launch_template_version
                .clone()
                .unwrap_or_else(|| "$Default".into());
            if let Some((image, itype, key, user)) =
                ec2_lt_version_data(req, Some(tpl_id), Some(tpl_name), &version)
            {
                group.launch_image_id = Some(image);
                group.launch_instance_type = Some(itype);
                group.launch_key_name = key;
                group.launch_user_data = user;
            }
        } else {
            group.launch_image_id = None;
            group.launch_instance_type = None;
            group.launch_key_name = None;
            group.launch_user_data = None;
        }
        if desired.is_some() {
            set_group_capacity(req, group, desired);
        }
    }
    Ok(String::new())
}

fn op_delete_auto_scaling_group(
    req: &Request,
    state: &mut AsgState,
    p: &Params,
) -> OpResult<String> {
    let name = p_string(p, "AutoScalingGroupName")
        .unwrap_or_default()
        .to_string();
    if !state.autoscaling_groups.contains_key(&name) {
        return Err(validation("AutoScalingGroup name not found - null"));
    }
    if let Some(mut group) = state.autoscaling_groups.remove(&name) {
        set_group_capacity(req, &mut group, Some(0));
    }
    // Successful mutate → CloudTrail management event on the default bus
    // (issue #347 allowlist); forceDelete only when the call carried it.
    emit_asg_cloudtrail(
        req,
        "DeleteAutoScalingGroup",
        [
            ("autoScalingGroupName", Some(serde_json::json!(name))),
            (
                "forceDelete",
                p_bool(p, "ForceDelete").map(|v| serde_json::json!(v)),
            ),
        ],
    );
    Ok(String::new())
}

/// CloudTrail management-event emit for the succeeded create/delete group
/// calls (issue #347). Members are `(member-name, Option<value>)` pairs so
/// the payload only carries fields the call actually had; fire-and-forget
/// through the process-global registry — never fails the API call.
fn emit_asg_cloudtrail(
    req: &Request,
    event_name: &str,
    members: impl IntoIterator<Item = (&'static str, Option<serde_json::Value>)>,
) {
    let mut params = serde_json::Map::new();
    for (name, value) in members {
        if let Some(value) = value {
            params.insert(name.to_string(), value);
        }
    }
    crate::cloudtrail_emit::emit_cloudtrail_api_call(
        req.account,
        &req.region,
        "autoscaling.amazonaws.com",
        event_name,
        &serde_json::Value::Object(params).to_string(),
    );
}

fn op_describe_auto_scaling_groups(state: &AsgState, p: &Params) -> OpResult<String> {
    let names: Vec<String> = p_list(p, "AutoScalingGroupNames");
    let filters = p_struct_list(p, "Filters");
    let groups: Vec<&Group> = state
        .autoscaling_groups
        .values()
        .filter(|g| group_pages_authorized(g, &names, &filters))
        .collect();
    let marker = p_string(p, "NextToken").unwrap_or_default();
    let mut start = 0usize;
    if !marker.is_empty() {
        if let Some(pos) = groups_marker(&groups, marker) {
            start = pos + 1;
        }
    }
    let max_records = p_i64(p, "MaxRecords").unwrap_or(50).max(1) as usize;
    let page: Vec<&Group> = groups
        .iter()
        .skip(start)
        .take(max_records)
        .copied()
        .collect();
    let mut body = list_xml(
        "AutoScalingGroups",
        &page
            .iter()
            .map(|g| group_member_xml_of(g))
            .collect::<Vec<_>>(),
    );
    if groups.len() > start + max_records {
        if let Some(last) = page.last() {
            body.push_str(&tag_esc("NextToken", &last.name));
        }
    }
    Ok(body)
}

/// Group member render with instance lookup through the shared name key.
fn group_member_xml_of(group: &Group) -> String {
    // A dummy state is enough for LC-name resolution: instance members read
    // their owning group by name, so hand the group itself in.
    group_member_xml_v2(group)
}

/// Group-embedded instance member.
fn group_member_xml_v2(group: &Group) -> String {
    let mut out = String::new();
    out.push_str(&tag_esc("AutoScalingGroupName", &group.name));
    out.push_str(&tag_esc("AutoScalingGroupARN", &group.arn()));
    if let Some(lc) = &group.launch_config_name {
        out.push_str(&tag_esc("LaunchConfigurationName", lc));
    }
    if let Some(lt_id) = group.launch_template_id.as_deref() {
        let mut lt = tag_esc("LaunchTemplateId", lt_id);
        if let Some(name) = group.launch_template_name.as_deref() {
            lt.push_str(&tag_esc("LaunchTemplateName", name));
        }
        if let Some(v) = group.provided_launch_template_version.as_deref() {
            lt.push_str(&tag_esc("Version", v));
        }
        out.push_str(&tag_raw("LaunchTemplate", &lt));
    }
    if let Some(mip) = &group.mixed_instances_policy {
        out.push_str(&tag_raw("MixedInstancesPolicy", &param_members(mip)));
    }
    out.push_str(&tag_esc(
        "MinSize",
        &group.min_size.unwrap_or(0).to_string(),
    ));
    out.push_str(&tag_esc(
        "MaxSize",
        &group.max_size.unwrap_or(0).to_string(),
    ));
    out.push_str(&tag_esc(
        "DesiredCapacity",
        &group.desired_capacity.to_string(),
    ));
    out.push_str(&tag_esc(
        "DefaultCooldown",
        &group.default_cooldown.to_string(),
    ));
    out.push_str(&list_scaped("AvailabilityZones", &group.availability_zones));
    out.push_str(&list_scaped(
        "LoadBalancerNames",
        &group.load_balancer_names,
    ));
    out.push_str(&list_scaped("TargetGroupARNs", &group.target_group_arns));
    out.push_str(&tag_esc("HealthCheckType", &group.health_check_type));
    if let Some(gp) = group.health_check_grace_period {
        out.push_str(&tag_esc("HealthCheckGracePeriod", &gp.to_string()));
    }
    out.push_str(&list_xml(
        "Instances",
        &group
            .instance_states
            .iter()
            .map(|i| asg_instance_member_of(group, i))
            .collect::<Vec<_>>(),
    ));
    out.push_str(&tag_esc("CreatedTime", &group.created_time));
    out.push_str(&suspended_xml(group));
    if let Some(pg) = &group.placement_group {
        out.push_str(&tag_esc("PlacementGroup", pg));
    }
    if let Some(vpc) = &group.vpc_zone_identifier {
        if !vpc.is_empty() {
            out.push_str(&tag_esc("VPCZoneIdentifier", vpc));
        }
    }
    out.push_str(&enabled_metrics_xml(group));
    out.push_str(&tags_xml(group));
    out.push_str(&list_scaped(
        "TerminationPolicies",
        &group.termination_policies,
    ));
    out.push_str(&tag_esc(
        "NewInstancesProtectedFromScaleIn",
        bool_text(group.new_instances_protected_from_scale_in),
    ));
    out.push_str(&tag_esc(
        "ServiceLinkedRoleARN",
        &group.service_linked_role_arn(),
    ));
    out.push_str(&tag_esc(
        "CapacityRebalance",
        bool_text(group.capacity_rebalance),
    ));
    if let Some(wp) = &group.warm_pool {
        out.push_str(&tag_raw("WarmPoolConfiguration", &warm_pool_xml(wp)));
    }
    out.push_str(&traffic_sources_xml(group));
    out
}

/// members-instances render (owning group passed in for LC/template rows).
fn asg_instance_member_of(group: &Group, inst: &AsgInstance) -> String {
    let mut out = String::new();
    out.push_str(&tag_esc("InstanceId", &inst.instance_id));
    out.push_str(&tag_esc("InstanceType", &inst.instance_type));
    out.push_str(&tag_esc("AvailabilityZone", &inst.az));
    out.push_str(&tag_esc("LifecycleState", &inst.lifecycle_state));
    out.push_str(&tag_esc("HealthStatus", &inst.health_status));
    if let Some(lc) = group.launch_config_name.as_deref() {
        out.push_str(&tag_esc("LaunchConfigurationName", lc));
    }
    if let Some(lt_id) = group.launch_template_id.as_deref() {
        let mut lt = tag_esc("LaunchTemplateId", lt_id);
        if let Some(name) = group.launch_template_name.as_deref() {
            lt.push_str(&tag_esc("LaunchTemplateName", name));
        }
        lt.push_str(&tag_esc(
            "Version",
            group
                .provided_launch_template_version
                .as_deref()
                .unwrap_or("$Default"),
        ));
        out.push_str(&tag_raw("LaunchTemplate", &lt));
    }
    out.push_str(&tag_esc(
        "ProtectedFromScaleIn",
        bool_text(inst.protected_from_scale_in),
    ));
    out
}

/// Standalone AutoScalingInstances member (adds AutoScalingGroupName).
fn standalone_instance_member(group_name: &str, group: &Group, inst: &AsgInstance) -> String {
    format!(
        "{}{}",
        tag_esc("AutoScalingGroupName", group_name),
        asg_instance_member_of(group, inst)
    )
}

/// describe_auto_scaling_groups tag filters (tag-key/tag-value/tag:key).
fn group_pages_authorized(g: &Group, names: &[String], filters: &[Params]) -> bool {
    group_filter_match(g, names, filters)
}

fn groups_marker(groups: &[&Group], marker: &str) -> Option<usize> {
    groups.iter().position(|g| g.name == marker)
}

fn group_filter_match(g: &Group, names: &[String], filters: &[Params]) -> bool {
    if !names.is_empty() && !names.iter().any(|n| n == &g.name) {
        return false;
    }
    'filters: for f in filters {
        let filter_name = elb_string(f, "Name").unwrap_or_default();
        let values = p_list_public(f, "Values");
        let has_tag = |check: &dyn Fn(&TagRecord) -> bool| g.tags.iter().any(check);
        if filter_name == "tag-key" {
            if !has_tag(&|t| values.contains(&t.key)) {
                return false;
            }
        } else if filter_name == "tag-value" {
            if !has_tag(&|t| {
                t.value
                    .as_deref()
                    .map(|v| values.contains(&v.to_string()))
                    .unwrap_or(false)
            }) {
                return false;
            }
        } else if let Some(key) = filter_name.strip_prefix("tag:") {
            if !has_tag(&|t| {
                t.key == key
                    && t.value
                        .as_deref()
                        .map(|v| values.contains(&v.to_string()))
                        .unwrap_or(false)
            }) {
                return false;
            }
        } else {
            // unknown filter names pass through like moto's fallthrough
            continue 'filters;
        }
    }
    true
}

/// p_list over a nested member map.
fn p_list_public(m: &Params, key: &str) -> Vec<String> {
    p_list(m, key)
}

fn op_set_desired_capacity(req: &Request, state: &mut AsgState, p: &Params) -> OpResult<String> {
    let name = p_string(p, "AutoScalingGroupName")
        .unwrap_or_default()
        .to_string();
    let desired = p_i64(p, "DesiredCapacity");
    let Some(group) = state.autoscaling_groups.get_mut(&name) else {
        return Err(validation("AutoScalingGroup name not found - null"));
    };
    set_group_capacity(req, group, desired);
    Ok(String::new())
}

fn op_describe_auto_scaling_instances(state: &AsgState, p: &Params) -> OpResult<String> {
    let ids: Vec<String> = p_list(p, "InstanceIds");
    let mut members: Vec<String> = Vec::new();
    for (name, group) in &state.autoscaling_groups {
        for inst in &group.instance_states {
            if !ids.is_empty() && !ids.contains(&inst.instance_id) {
                continue;
            }
            members.push(standalone_instance_member(name, group, inst));
        }
    }
    Ok(list_xml("AutoScalingInstances", &members))
}

// ---------------------------------------------------------------------------
// Attach / detach / health / standby / terminate / suspend / protection.
// ---------------------------------------------------------------------------

fn op_attach_instances(req: &Request, state: &mut AsgState, p: &Params) -> OpResult<String> {
    let group_name = p_string(p, "AutoScalingGroupName")
        .unwrap_or_default()
        .to_string();
    let instance_ids: Vec<String> = p_list(p, "InstanceIds");
    let Some(group) = state.autoscaling_groups.get_mut(&group_name) else {
        return Err(validation("AutoScalingGroup name not found - null"));
    };
    if group.active_instances().len() + instance_ids.len() > group.max_size.unwrap_or(0) as usize {
        return Err(resource_contention());
    }
    for id in &instance_ids {
        if ec2_get_instance(req, id).is_none() {
            return Err(AsgError::new(
                400,
                "InvalidInstanceID.NotFound",
                format!("The instance ID '{id}' does not exist"),
            ));
        }
    }
    group.desired_capacity = group.active_instances().len() as i64 + instance_ids.len() as i64;
    for id in instance_ids {
        group.instance_states.push(AsgInstance {
            lifecycle_state: "InService".into(),
            health_status: "Healthy".into(),
            protected_from_scale_in: group.new_instances_protected_from_scale_in,
            az: ec2_get_instance(req, &id).map(|v| v.az).unwrap_or_default(),
            instance_id: id.clone(),
            instance_type: ec2_get_instance(req, &id)
                .map(|v| v.instance_type)
                .unwrap_or_default(),
        });
        create_tags_ec2(
            req,
            vec![
                ("Action".into(), "CreateTags".into()),
                ("ResourceId.1".into(), id.clone()),
                ("Tag.1.Key".into(), ASG_NAME_TAG.into()),
                ("Tag.1.Value".into(), group_name.clone()),
            ],
        );
    }
    sync_attached_elbs(req, group);
    sync_target_groups(req, group);
    Ok(String::new())
}

/// Activity member for a moved instance (detach flavor), moto's fields.
fn detach_activity(group: &Group, instance_id: &str) -> String {
    let mut out = String::new();
    out.push_str(&tag_esc("ActivityId", &uuid::Uuid::new_v4().to_string()));
    out.push_str(&tag_esc("AutoScalingGroupName", &group.name));
    out.push_str(&tag_esc(
        "Description",
        &format!("Detaching EC2 instance: {instance_id}"),
    ));
    out.push_str(&tag_esc(
        "Cause",
        &format!(
            "At {}, instance {instance_id} was detached in response to a user request.",
            now_moto_str()
        ),
    ));
    out.push_str(&tag_esc("StartTime", &now_wire_ts()));
    out.push_str(&tag_esc("EndTime", &now_wire_ts()));
    out.push_str(&tag_esc("StatusCode", "InProgress"));
    out.push_str(&tag_esc("Progress", "50"));
    out
}

fn op_detach_instances(req: &Request, state: &mut AsgState, p: &Params) -> OpResult<String> {
    let group_name = p_string(p, "AutoScalingGroupName")
        .unwrap_or_default()
        .to_string();
    let instance_ids: Vec<String> = p_list(p, "InstanceIds");
    let should_decrement = p_bool(p, "ShouldDecrementDesiredCapacity").unwrap_or(false);
    let Some(group) = state.autoscaling_groups.get_mut(&group_name) else {
        return Err(validation("AutoScalingGroup name not found - null"));
    };
    let original_capacity = group.desired_capacity;
    let mut activities = Vec::new();
    for id in &instance_ids {
        delete_asg_tag_ec2(req, id, &group_name);
        activities.push(detach_activity(group, id));
    }
    group
        .instance_states
        .retain(|x| !instance_ids.contains(&x.instance_id));
    if should_decrement {
        group.desired_capacity = original_capacity - instance_ids.len() as i64;
    }
    let capacity = group.desired_capacity;
    set_group_capacity(req, group, Some(capacity));
    let body = list_xml("Activities", &activities);
    Ok(body)
}

fn op_set_instance_health(_req: &Request, state: &mut AsgState, p: &Params) -> OpResult<String> {
    let instance_id = p_string(p, "InstanceId").unwrap_or_default().to_string();
    let health_status = p_string(p, "HealthStatus").unwrap_or_default().to_string();
    if health_status != "Healthy" && health_status != "Unhealthy" {
        return Err(validation(
            "Valid instance health states are: [Healthy, Unhealthy]",
        ));
    }
    let mut updated = false;
    for group in state.autoscaling_groups.values_mut() {
        for inst in &mut group.instance_states {
            if inst.instance_id == instance_id {
                inst.health_status = health_status.clone();
                updated = true;
            }
        }
    }
    if !updated {
        return Err(validation(format!("Instance [{instance_id}] not found.")));
    }
    Ok(String::new())
}

fn op_enter_standby(req: &Request, state: &mut AsgState, p: &Params) -> OpResult<String> {
    let group_name = p_string(p, "AutoScalingGroupName")
        .unwrap_or_default()
        .to_string();
    let instance_ids: Vec<String> = p_list(p, "InstanceIds");
    let should_decrement = p_bool(p, "ShouldDecrementDesiredCapacity").unwrap_or(false);
    let Some(group) = state.autoscaling_groups.get_mut(&group_name) else {
        return Err(validation("AutoScalingGroup name not found - null"));
    };
    let mut activities = Vec::new();
    let mut moved: Vec<String> = Vec::new();
    for inst in &mut group.instance_states {
        if instance_ids.contains(&inst.instance_id) {
            inst.lifecycle_state = "Standby".into();
            moved.push(inst.instance_id.clone());
        }
    }
    let original = group.desired_capacity;
    if should_decrement && !moved.is_empty() {
        group.desired_capacity -= moved.len() as i64;
    }
    for instance_id in &moved {
        activities.push(standby_activity(
            group,
            instance_id,
            original,
            should_decrement,
        ));
    }
    let capacity = group.desired_capacity;
    set_group_capacity(req, group, Some(capacity));
    Ok(list_xml("Activities", &activities))
}

fn standby_activity(group: &Group, instance_id: &str, original: i64, decrement: bool) -> String {
    let shrink_note = if decrement {
        format!(
            ", shrinking the capacity from {original} to {}.",
            group.desired_capacity
        )
    } else {
        ".".to_string()
    };
    let mut out = String::new();
    out.push_str(&tag_esc("ActivityId", &uuid::Uuid::new_v4().to_string()));
    out.push_str(&tag_esc("AutoScalingGroupName", &group.name));
    out.push_str(&tag_esc(
        "Description",
        &format!("Moving EC2 instance to StandBy: {instance_id}"),
    ));
    out.push_str(&tag_esc(
        "Cause",
        &format!(
            "At {}, instance {instance_id} was moved to standby in response to a user request{}",
            now_moto_str(),
            shrink_note
        ),
    ));
    out.push_str(&tag_esc("StartTime", &now_wire_ts()));
    out.push_str(&tag_esc("EndTime", &now_wire_ts()));
    out.push_str(&tag_esc("StatusCode", "InProgress"));
    out.push_str(&tag_esc("Progress", "50"));
    out
}

fn op_exit_standby(req: &Request, state: &mut AsgState, p: &Params) -> OpResult<String> {
    let group_name = p_string(p, "AutoScalingGroupName")
        .unwrap_or_default()
        .to_string();
    let instance_ids: Vec<String> = p_list(p, "InstanceIds");
    let Some(group) = state.autoscaling_groups.get_mut(&group_name) else {
        return Err(validation("AutoScalingGroup name not found - null"));
    };
    let mut activities = Vec::new();
    let mut moved: Vec<String> = Vec::new();
    for inst in &mut group.instance_states {
        if instance_ids.contains(&inst.instance_id) {
            inst.lifecycle_state = "InService".into();
            moved.push(inst.instance_id.clone());
        }
    }
    let original = group.desired_capacity;
    group.desired_capacity += moved.len() as i64;
    for instance_id in &moved {
        activities.push(exit_standby_activity(group, instance_id, original));
    }
    let capacity = group.desired_capacity;
    set_group_capacity(req, group, Some(capacity));
    Ok(list_xml("Activities", &activities))
}

/// ExitStandby Activity (moto renders PreInService with progress 30).
fn exit_standby_activity(group: &Group, instance_id: &str, original: i64) -> String {
    let mut out = String::new();
    out.push_str(&tag_esc("ActivityId", &uuid::Uuid::new_v4().to_string()));
    out.push_str(&tag_esc("AutoScalingGroupName", &group.name));
    out.push_str(&tag_esc(
        "Description",
        &format!("Moving EC2 instance out of StandBy: {instance_id}"),
    ));
    out.push_str(&tag_esc(
        "Cause",
        &format!(
            "At {}, instance {instance_id} was moved out of standby in response to a user request, increasing the capacity from {original} to {}.",
            now_moto_str(),
            group.desired_capacity
        ),
    ));
    out.push_str(&tag_esc("StartTime", &now_wire_ts()));
    out.push_str(&tag_esc("EndTime", &now_wire_ts()));
    out.push_str(&tag_esc("StatusCode", "PreInService"));
    out.push_str(&tag_esc("Progress", "30"));
    out
}

fn op_terminate_instance(req: &Request, state: &mut AsgState, p: &Params) -> OpResult<String> {
    let instance_id = p_string(p, "InstanceId").unwrap_or_default().to_string();
    let should_decrement = p_bool(p, "ShouldDecrementDesiredCapacity").unwrap_or(false);
    // find the owning group (moto iterates all groups)
    let mut owner: Option<String> = None;
    for (name, group) in &state.autoscaling_groups {
        if group
            .instance_states
            .iter()
            .any(|x| x.instance_id == instance_id)
        {
            owner = Some(name.clone());
            break;
        }
    }
    let Some(group_name) = owner else {
        return Err(validation(format!("Instance [{instance_id}] not found.")));
    };
    let Some(group) = state.autoscaling_groups.get_mut(&group_name) else {
        return Err(validation(format!("Instance [{instance_id}] not found.")));
    };
    let original_capacity = group.desired_capacity;
    let remaining = group
        .instance_states
        .iter()
        .filter(|x| x.instance_id != instance_id)
        .count() as i64;
    group
        .instance_states
        .retain(|x| x.instance_id != instance_id);
    if should_decrement {
        group.desired_capacity = remaining;
    }
    let capacity = group.desired_capacity;
    set_group_capacity(req, group, Some(capacity));
    delete_asg_tag_ec2(req, &instance_id, &group_name);
    terminate_ec2(req, std::slice::from_ref(&instance_id));
    let mut out = String::new();
    out.push_str(&tag_esc("ActivityId", &uuid::Uuid::new_v4().to_string()));
    out.push_str(&tag_esc("AutoScalingGroupName", &group_name));
    out.push_str(&tag_esc(
        "Description",
        &format!("Terminating EC2 instance: {instance_id}"),
    ));
    let shrink = if capacity < original_capacity {
        format!(", shrinking the capacity from {original_capacity} to {capacity}.")
    } else {
        ".".to_string()
    };
    out.push_str(&tag_esc(
        "Cause",
        &format!(
            "At {}, instance {instance_id} was taken out of service in response to a user request{}",
            now_moto_str(),
            shrink
        ),
    ));
    out.push_str(&tag_esc("StartTime", &now_wire_ts()));
    out.push_str(&tag_esc("EndTime", &now_wire_ts()));
    out.push_str(&tag_esc("StatusCode", "InProgress"));
    out.push_str(&tag_esc("Progress", "0"));
    Ok(tag_raw("Activity", &out))
}

fn op_suspend_processes(state: &mut AsgState, p: &Params) -> OpResult<String> {
    let group_name = p_string(p, "AutoScalingGroupName")
        .unwrap_or_default()
        .to_string();
    let requested = p_list(p, "ScalingProcesses");
    let Some(group) = state.autoscaling_groups.get_mut(&group_name) else {
        return Err(validation("AutoScalingGroup name not found - null"));
    };
    let mut to_add: Vec<String> = if requested.is_empty() {
        PROCESS_NAMES.iter().map(|s| s.to_string()).collect()
    } else {
        requested
    };
    for name in to_add.drain(..) {
        if !group.suspended_processes.contains(&name) {
            group.suspended_processes.push(name);
        }
    }
    // store in the PROCESS_NAMES catalog order (moto sorts through set())
    group.suspended_processes.sort_by_key(|name| {
        PROCESS_NAMES
            .iter()
            .position(|p| p == name)
            .unwrap_or(usize::MAX)
    });
    Ok(String::new())
}

fn op_resume_processes(state: &mut AsgState, p: &Params) -> OpResult<String> {
    let group_name = p_string(p, "AutoScalingGroupName")
        .unwrap_or_default()
        .to_string();
    let requested = p_list(p, "ScalingProcesses");
    let Some(group) = state.autoscaling_groups.get_mut(&group_name) else {
        return Err(validation("AutoScalingGroup name not found - null"));
    };
    if requested.is_empty() {
        group.suspended_processes.clear();
    } else {
        group.suspended_processes.retain(|x| !requested.contains(x));
    }
    Ok(String::new())
}

fn op_set_instance_protection(state: &mut AsgState, p: &Params) -> OpResult<String> {
    let group_name = p_string(p, "AutoScalingGroupName")
        .unwrap_or_default()
        .to_string();
    let instance_ids: Vec<String> = p_list(p, "InstanceIds");
    let protected = p_bool(p, "ProtectedFromScaleIn").unwrap_or(false);
    let Some(group) = state.autoscaling_groups.get_mut(&group_name) else {
        return Err(validation("AutoScalingGroup name not found - null"));
    };
    for inst in &mut group.instance_states {
        if instance_ids.contains(&inst.instance_id) {
            inst.protected_from_scale_in = protected;
        }
    }
    Ok(String::new())
}

// ---------------------------------------------------------------------------
// Tags.
// ---------------------------------------------------------------------------

fn op_create_or_update_tags(state: &mut AsgState, p: &Params) -> OpResult<String> {
    for tag in p_struct_list(p, "Tags") {
        let resource_id = elb_string(&tag, "ResourceId")
            .unwrap_or_default()
            .to_string();
        let Some(group) = state.autoscaling_groups.get_mut(&resource_id) else {
            return Err(validation(format!(
                "AutoScalingGroup name not found - {resource_id}"
            )));
        };
        let new_key = elb_string(&tag, "Key").unwrap_or_default().to_string();
        let mut new_tags: Vec<TagRecord> = Vec::new();
        for old in &group.tags {
            if old.key == new_key {
                new_tags.extend(normalize_group_tags(
                    &group.name,
                    std::slice::from_ref(&tag),
                ));
            } else {
                new_tags.push(old.clone());
            }
        }
        if !new_tags.iter().any(|t| t.key == new_key) {
            new_tags.extend(normalize_group_tags(
                &group.name,
                std::slice::from_ref(&tag),
            ));
        }
        group.tags = new_tags;
    }
    Ok(String::new())
}

fn op_delete_tags(state: &mut AsgState, p: &Params) -> OpResult<String> {
    for tag in p_struct_list(p, "Tags") {
        let resource_id = elb_string(&tag, "ResourceId")
            .unwrap_or_default()
            .to_string();
        let key = elb_string(&tag, "Key").unwrap_or_default().to_string();
        let Some(group) = state.autoscaling_groups.get_mut(&resource_id) else {
            return Err(validation(format!(
                "AutoScalingGroup name not found - {resource_id}"
            )));
        };
        group.tags.retain(|old| old.key != key);
    }
    Ok(String::new())
}

fn op_describe_tags(state: &AsgState, p: &Params) -> OpResult<String> {
    let mut tags: Vec<&TagRecord> = state
        .autoscaling_groups
        .values()
        .flat_map(|g| g.tags.iter())
        .collect();
    let filters = p_struct_list(p, "Filters");
    for f in filters {
        let name = elb_string(&f, "Name").unwrap_or_default();
        let values = p_list(&f, "Values");
        match name {
            "auto-scaling-group" => tags.retain(|t| values.contains(&t.resource_id)),
            "propagate-at-launch" => tags.retain(|t| {
                // moto compares python str(True)/str(False) spelling
                let spelled = if t.propagate {
                    "True".to_string()
                } else {
                    "False".to_string()
                };
                values.contains(&spelled)
            }),
            "key" => tags.retain(|t| values.contains(&t.key)),
            "value" => tags.retain(|t| {
                t.value
                    .as_deref()
                    .map(|v| values.contains(&v.to_string()))
                    .unwrap_or(false)
            }),
            _ => {}
        }
    }
    let members: Vec<String> = tags
        .iter()
        .map(|t| {
            format!(
                "{}{}{}{}{}",
                tag_esc("ResourceId", &t.resource_id),
                tag_esc("ResourceType", &t.resource_type),
                tag_esc("Key", &t.key),
                tag_opt("Value", t.value.as_deref()),
                tag_esc("PropagateAtLaunch", bool_text(t.propagate)),
            )
        })
        .collect();
    Ok(list_xml("Tags", &members))
}

// ---------------------------------------------------------------------------
// Metrics collection.
// ---------------------------------------------------------------------------

fn op_enable_metrics_collection(state: &mut AsgState, p: &Params) -> OpResult<String> {
    let name = p_string(p, "AutoScalingGroupName")
        .unwrap_or_default()
        .to_string();
    let Some(group) = state.autoscaling_groups.get_mut(&name) else {
        return Err(validation("AutoScalingGroup name not found - null"));
    };
    group.metrics = p_list(p, "Metrics");
    Ok(String::new())
}

fn op_disable_metrics_collection(state: &mut AsgState, p: &Params) -> OpResult<String> {
    let name = p_string(p, "AutoScalingGroupName")
        .unwrap_or_default()
        .to_string();
    let metrics = p_list(p, "Metrics");
    let Some(group) = state.autoscaling_groups.get_mut(&name) else {
        return Err(validation("AutoScalingGroup name not found - null"));
    };
    if metrics.is_empty() {
        group.metrics.clear();
    } else {
        group.metrics.retain(|m| !metrics.contains(m));
    }
    Ok(String::new())
}

// ---------------------------------------------------------------------------
// Warm pool.
// ---------------------------------------------------------------------------

fn op_put_warm_pool(state: &mut AsgState, p: &Params) -> OpResult<String> {
    let name = p_string(p, "AutoScalingGroupName")
        .unwrap_or_default()
        .to_string();
    let Some(group) = state.autoscaling_groups.get_mut(&name) else {
        return Err(validation("AutoScalingGroup name not found - null"));
    };
    group.warm_pool = Some(WarmPool {
        max_group_prepared_capacity: p_string(p, "MaxGroupPreparedCapacity")
            .map(std::string::ToString::to_string),
        min_size: p_string(p, "MinSize")
            .filter(|v| !v.is_empty())
            .unwrap_or("0")
            .to_string(),
        pool_state: p_string(p, "PoolState")
            .filter(|v| !v.is_empty())
            .unwrap_or("Stopped")
            .to_string(),
        instance_reuse_policy: p
            .get("InstanceReusePolicy")
            .and_then(raw_map_ref)
            .filter(|m| !m.is_empty())
            .cloned(),
    });
    Ok(String::new())
}

fn op_describe_warm_pool(state: &AsgState, p: &Params) -> OpResult<String> {
    let name = p_string(p, "AutoScalingGroupName")
        .unwrap_or_default()
        .to_string();
    let Some(group) = state.autoscaling_groups.get(&name) else {
        return Err(validation("AutoScalingGroup name not found - null"));
    };
    let mut body = String::new();
    if let Some(wp) = &group.warm_pool {
        body.push_str(&tag_raw("WarmPoolConfiguration", &warm_pool_xml(wp)));
    }
    body.push_str("<Instances/>");
    Ok(body)
}

fn op_delete_warm_pool(state: &mut AsgState, p: &Params) -> OpResult<String> {
    let name = p_string(p, "AutoScalingGroupName")
        .unwrap_or_default()
        .to_string();
    let Some(group) = state.autoscaling_groups.get_mut(&name) else {
        return Err(validation("AutoScalingGroup name not found - null"));
    };
    group.warm_pool = None;
    Ok(String::new())
}

// ---------------------------------------------------------------------------
// Lifecycle hooks.
// ---------------------------------------------------------------------------

fn op_put_lifecycle_hook(state: &mut AsgState, p: &Params) -> OpResult<String> {
    let name = p_string(p, "LifecycleHookName")
        .unwrap_or_default()
        .to_string();
    let as_name = p_string(p, "AutoScalingGroupName")
        .unwrap_or_default()
        .to_string();
    if !state.autoscaling_groups.contains_key(&as_name) {
        return Err(group_not_found(&as_name));
    }
    let hook = LifecycleHook {
        name: name.clone(),
        auto_scaling_group_name: as_name.clone(),
        lifecycle_transition: p_string(p, "LifecycleTransition")
            .map(std::string::ToString::to_string),
        heartbeat_timeout: p_i64(p, "HeartbeatTimeout").unwrap_or(3600),
        default_result: p_string(p, "DefaultResult")
            .unwrap_or("ABANDON")
            .to_string(),
    };
    state
        .lifecycle_hooks
        .insert(format!("{as_name}_{name}"), hook);
    Ok(String::new())
}

fn op_describe_lifecycle_hooks(state: &AsgState, p: &Params) -> OpResult<String> {
    let as_name = p_string(p, "AutoScalingGroupName")
        .unwrap_or_default()
        .to_string();
    let hook_names: Vec<String> = p_list(p, "LifecycleHookNames");
    let members: Vec<String> = state
        .lifecycle_hooks
        .values()
        .filter(|h| h.auto_scaling_group_name == as_name)
        .filter(|h| hook_names.is_empty() || hook_names.contains(&h.name))
        .map(hook_xml_public)
        .collect();
    Ok(list_xml("LifecycleHooks", &members))
}

fn hook_xml_public(h: &LifecycleHook) -> String {
    hook_xml(h)
}

fn op_delete_lifecycle_hook(state: &mut AsgState, p: &Params) -> OpResult<String> {
    let name = p_string(p, "LifecycleHookName").unwrap_or_default();
    let as_name = p_string(p, "AutoScalingGroupName").unwrap_or_default();
    state.lifecycle_hooks.remove(&format!("{as_name}_{name}"));
    Ok(String::new())
}

/// Shared validation for the two lifecycle-action operations (fork checks the
/// group, then the hook).
fn lifecycle_common_valid(state: &AsgState, p: &Params) -> Result<String, AsgError> {
    let group_name = p_string(p, "AutoScalingGroupName")
        .unwrap_or_default()
        .to_string();
    if !state.autoscaling_groups.contains_key(&group_name) {
        return Err(group_not_found(&group_name));
    }
    let hook_name = p_string(p, "LifecycleHookName")
        .unwrap_or_default()
        .to_string();
    let known = state
        .lifecycle_hooks
        .values()
        .any(|h| h.auto_scaling_group_name == group_name && h.name == hook_name);
    if !known {
        return Err(validation(format!(
            "No Lifecycle Hook found with name '{hook_name}'"
        )));
    }
    Ok(group_name)
}

fn op_complete_lifecycle_action(state: &AsgState, p: &Params) -> OpResult<String> {
    lifecycle_common_valid(state, p)?;
    Ok(String::new())
}

fn op_record_lifecycle_action_heartbeat(state: &AsgState, p: &Params) -> OpResult<String> {
    lifecycle_common_valid(state, p)?;
    Ok(String::new())
}

// ---------------------------------------------------------------------------
// Scaling policies.
// ---------------------------------------------------------------------------

fn op_put_scaling_policy(
    state: &mut AsgState,
    p: &Params,
    account: u64,
    region: &str,
) -> OpResult<String> {
    let name = p_string(p, "PolicyName").unwrap_or_default().to_string();
    let as_name = p_string(p, "AutoScalingGroupName")
        .unwrap_or_default()
        .to_string();
    let policy_type = p_string(p, "PolicyType")
        .unwrap_or("SimpleScaling")
        .to_string();
    let cooldown = if policy_type == "SimpleScaling" {
        Some(p_i64(p, "Cooldown").unwrap_or(DEFAULT_COOLDOWN))
    } else {
        None
    };
    let target_tracking_config = if policy_type == "TargetTrackingScaling" {
        p.get("TargetTrackingConfiguration")
            .and_then(raw_map_ref)
            .filter(|m| !m.is_empty())
            .cloned()
    } else {
        None
    };
    let step_adjustments = p_struct_list(p, "StepAdjustments");
    let predictive: Option<Params> = Some(
        p.get("PredictiveScalingConfiguration")
            .and_then(raw_map_ref)
            .cloned()
            .unwrap_or_default(),
    );
    // moto's policy ARN region/account are this backend's
    let final_arn = format!(
        "arn:{}:autoscaling:{}:{}:scalingPolicy:{}:autoScalingGroupName/{}:policyName/{}",
        partition(region),
        region,
        account,
        POLICY_ARN_UUID,
        as_name,
        name,
    );
    // keep the hardcoded policy-ARN marker rendering consistent
    let policy = ScalingPolicy {
        name: name.clone(),
        policy_type,
        metric_aggregation_type: p_string(p, "MetricAggregationType")
            .map(std::string::ToString::to_string),
        adjustment_type: p_string(p, "AdjustmentType").map(std::string::ToString::to_string),
        auto_scaling_group_name: as_name,
        min_adjustment_magnitude: p_string(p, "MinAdjustmentMagnitude")
            .map(std::string::ToString::to_string),
        scaling_adjustment: p_i64(p, "ScalingAdjustment"),
        cooldown,
        target_tracking_config,
        step_adjustments,
        estimated_instance_warmup: p_string(p, "EstimatedInstanceWarmup")
            .map(std::string::ToString::to_string),
        predictive_scaling_configuration: predictive,
    };
    let arn = final_arn;
    let _ = arn.clone();
    state.policies.insert(name, policy);
    Ok(tag_esc("PolicyARN", &arn))
}

fn op_describe_policies(
    state: &AsgState,
    p: &Params,
    region: &str,
    account: u64,
) -> OpResult<String> {
    let as_name = p_string(p, "AutoScalingGroupName").map(std::string::ToString::to_string);
    let policy_names: Vec<String> = p_list(p, "PolicyNames");
    let policy_types: Vec<String> = p_list(p, "PolicyTypes");
    let members: Vec<String> = state
        .policies
        .values()
        .filter(|pol| {
            as_name
                .as_deref()
                .map(|n| n == pol.auto_scaling_group_name)
                .unwrap_or(true)
        })
        .filter(|pol| policy_names.is_empty() || policy_names.contains(&pol.name))
        .filter(|pol| policy_types.is_empty() || policy_types.contains(&pol.policy_type))
        .map(|pol| policy_xml(pol, region, account))
        .collect();
    Ok(list_xml("ScalingPolicies", &members))
}

fn op_delete_policy(state: &mut AsgState, p: &Params) -> OpResult<String> {
    let name = p_string(p, "PolicyName").unwrap_or_default();
    state.policies.remove(name);
    Ok(String::new())
}

fn op_execute_policy(req: &Request, state: &mut AsgState, p: &Params) -> OpResult<String> {
    let name = p_string(p, "PolicyName").unwrap_or_default().to_string();
    let Some(policy) = state.policies.get(&name) else {
        return Err(validation(format!("Policy not found - {name}")));
    };
    let policy = policy.clone();
    execute_policy_change(req, state, &policy);
    Ok(String::new())
}

// ---------------------------------------------------------------------------
// Scheduled actions.
// ---------------------------------------------------------------------------

fn put_scheduled_action(state: &mut AsgState, as_name: &str, action: &Params) {
    let scheduled_name = elb_string(action, "ScheduledActionName")
        .unwrap_or_default()
        .to_string();
    let stored = ScheduledAction {
        auto_scaling_group_name: as_name.to_string(),
        desired_capacity: elb_i64(action, "DesiredCapacity"),
        max_size: elb_i64(action, "MaxSize"),
        min_size: elb_i64(action, "MinSize"),
        scheduled_action_name: scheduled_name.clone(),
        start_time: elb_string(action, "StartTime").map(std::string::ToString::to_string),
        end_time: elb_string(action, "EndTime").map(std::string::ToString::to_string),
        recurrence: elb_string(action, "Recurrence").map(std::string::ToString::to_string),
        time_zone: elb_string(action, "TimeZone").map(std::string::ToString::to_string),
    };
    state.scheduled_actions.insert(scheduled_name, stored);
}

fn op_put_scheduled_update_group_action(state: &mut AsgState, p: &Params) -> OpResult<String> {
    let as_name = p_string(p, "AutoScalingGroupName")
        .unwrap_or_default()
        .to_string();
    put_scheduled_action(state, &as_name, p);
    Ok(String::new())
}

fn op_batch_put_scheduled_update_group_action(
    state: &mut AsgState,
    p: &Params,
) -> OpResult<String> {
    let as_name = p_string(p, "AutoScalingGroupName")
        .unwrap_or_default()
        .to_string();
    for action in p_struct_list(p, "ScheduledUpdateGroupActions") {
        put_scheduled_action(state, &as_name, &action);
    }
    // moto's batch path only reports AutoscalingClientError failures, and the
    // backend put never raises them here.
    Ok("<FailedScheduledUpdateGroupActions/>".to_string())
}

#[derive(Clone, Debug)]
struct FailedAction {
    scheduled_action_name: String,
    error_code: Option<String>,
    error_message: Option<String>,
}

fn failed_action_xml(f: &FailedAction) -> String {
    let mut out = tag_esc("ScheduledActionName", &f.scheduled_action_name);
    if let Some(code) = f.error_code.as_deref() {
        out.push_str(&tag_esc("ErrorCode", code));
    }
    if let Some(msg) = f.error_message.as_deref() {
        out.push_str(&tag_esc("ErrorMessage", msg));
    }
    out
}

fn op_delete_scheduled_action(state: &mut AsgState, p: &Params) -> OpResult<String> {
    let as_name = p_string(p, "AutoScalingGroupName")
        .unwrap_or_default()
        .to_string();
    let scheduled_name = p_string(p, "ScheduledActionName")
        .unwrap_or_default()
        .to_string();
    let known = state
        .scheduled_actions
        .get(&scheduled_name)
        .map(|sa| sa.auto_scaling_group_name == as_name);
    if known.unwrap_or(false) {
        state.scheduled_actions.remove(&scheduled_name);
    } else {
        return Err(validation("Scheduled action name not found"));
    }
    Ok(String::new())
}

fn op_batch_delete_scheduled_action(state: &mut AsgState, p: &Params) -> OpResult<String> {
    let as_name = p_string(p, "AutoScalingGroupName")
        .unwrap_or_default()
        .to_string();
    let names = p_list(p, "ScheduledActionNames");
    let mut failed = Vec::new();
    for scheduled_name in names {
        let known = state
            .scheduled_actions
            .get(&scheduled_name)
            .map(|sa| sa.auto_scaling_group_name == as_name)
            .unwrap_or(false);
        if known {
            state.scheduled_actions.remove(&scheduled_name);
        } else {
            failed.push(FailedAction {
                scheduled_action_name: scheduled_name,
                error_code: Some("ValidationError".to_string()),
                error_message: Some("Scheduled action name not found".to_string()),
            });
        }
    }
    let members: Vec<String> = failed.iter().map(failed_action_xml).collect();
    Ok(list_xml("FailedScheduledActions", &members))
}

fn op_describe_scheduled_actions(state: &AsgState, p: &Params) -> OpResult<String> {
    let as_name = p_string(p, "AutoScalingGroupName").map(std::string::ToString::to_string);
    let scheduled_names: Vec<String> = p_list(p, "ScheduledActionNames");
    let members: Vec<String> = state
        .scheduled_actions
        .values()
        .filter(|sa| {
            as_name
                .as_deref()
                .map(|n| sa.auto_scaling_group_name == n)
                .unwrap_or(true)
        })
        .filter(|sa| {
            scheduled_names.is_empty() || scheduled_names.contains(&sa.scheduled_action_name)
        })
        .map(scheduled_xml_public)
        .collect();
    Ok(list_xml("ScheduledUpdateGroupActions", &members))
}

fn scheduled_xml_public(a: &ScheduledAction) -> String {
    scheduled_xml(a)
}

// ---------------------------------------------------------------------------
// Traffic sources.
// ---------------------------------------------------------------------------

fn op_attach_traffic_sources(state: &mut AsgState, p: &Params) -> OpResult<String> {
    let as_name = p_string(p, "AutoScalingGroupName")
        .unwrap_or_default()
        .to_string();
    let Some(group) = state.autoscaling_groups.get_mut(&as_name) else {
        return Err(group_not_found(&as_name));
    };
    for source in p_struct_list(p, "TrafficSources") {
        let identifier = elb_string(&source, "Identifier")
            .unwrap_or_default()
            .to_string();
        if identifier.is_empty() {
            continue;
        }
        let existing = group
            .traffic_sources
            .iter()
            .any(|ts| elb_string(ts, "Identifier") == Some(identifier.as_str()));
        if !existing {
            group.traffic_sources.push(source.clone());
        }
    }
    Ok(String::new())
}

fn op_describe_traffic_sources(state: &AsgState, p: &Params) -> OpResult<String> {
    let as_name = p_string(p, "AutoScalingGroupName")
        .unwrap_or_default()
        .to_string();
    let source_type = p_string(p, "TrafficSourceType").map(std::string::ToString::to_string);
    let Some(group) = state.autoscaling_groups.get(&as_name) else {
        return Err(group_not_found(&as_name));
    };
    let members: Vec<String> = group
        .traffic_sources
        .iter()
        .filter(|ts| {
            source_type
                .as_deref()
                .map(|t| elb_string(ts, "Type") == Some(t))
                .unwrap_or(true)
        })
        .map(|ts| {
            (
                "InService",
                elb_string(ts, "Identifier").unwrap_or_default(),
                elb_string(ts, "Type").unwrap_or_default(),
            )
        })
        .map(|(state_name, id, ts_type)| {
            format!(
                "{}{}{}",
                tag_esc("State", state_name),
                tag_esc("Identifier", id),
                if ts_type.is_empty() {
                    "<Type/>".to_string()
                } else {
                    tag_esc("Type", ts_type)
                },
            )
        })
        .collect();
    Ok(list_xml("TrafficSources", &members))
}

fn op_detach_traffic_sources(state: &mut AsgState, p: &Params) -> OpResult<String> {
    let as_name = p_string(p, "AutoScalingGroupName")
        .unwrap_or_default()
        .to_string();
    let identifiers: Vec<String> = p_struct_list(p, "TrafficSources")
        .iter()
        .filter_map(|ts| elb_string(ts, "Identifier").map(std::string::ToString::to_string))
        .collect();
    let Some(group) = state.autoscaling_groups.get_mut(&as_name) else {
        return Err(group_not_found(&as_name));
    };
    group.traffic_sources.retain(|ts| {
        !elb_string(ts, "Identifier")
            .map(|v| identifiers.contains(&v.to_string()))
            .unwrap_or(false)
    });
    Ok(String::new())
}

// ---------------------------------------------------------------------------
// Classic load balancers and target groups.
// ---------------------------------------------------------------------------

fn op_attach_load_balancers(req: &Request, state: &mut AsgState, p: &Params) -> OpResult<String> {
    let as_name = p_string(p, "AutoScalingGroupName")
        .unwrap_or_default()
        .to_string();
    let names = p_list(p, "LoadBalancerNames");
    let Some(group) = state.autoscaling_groups.get_mut(&as_name) else {
        return Err(validation("AutoScalingGroup name not found - null"));
    };
    for name in names {
        if !group.load_balancer_names.contains(&name) {
            group.load_balancer_names.push(name);
        }
    }
    sync_attached_elbs(req, group);
    Ok(String::new())
}

fn op_describe_load_balancers(state: &AsgState, p: &Params) -> OpResult<String> {
    let as_name = p_string(p, "AutoScalingGroupName")
        .unwrap_or_default()
        .to_string();
    let Some(group) = state.autoscaling_groups.get(&as_name) else {
        return Err(validation("AutoScalingGroup name not found - null"));
    };
    let members: Vec<String> = group
        .load_balancer_names
        .iter()
        .map(|n| {
            format!(
                "{}{}",
                tag_esc("LoadBalancerName", n),
                tag_esc("State", "Added")
            )
        })
        .collect();
    Ok(list_xml("LoadBalancers", &members))
}

fn op_detach_load_balancers(req: &Request, state: &mut AsgState, p: &Params) -> OpResult<String> {
    let as_name = p_string(p, "AutoScalingGroupName")
        .unwrap_or_default()
        .to_string();
    let names = p_list(p, "LoadBalancerNames");
    let Some(group) = state.autoscaling_groups.get_mut(&as_name) else {
        return Err(validation("AutoScalingGroup name not found - null"));
    };
    // moto deregisters every current group instance from all attached ELBs
    let ids: Vec<String> = group
        .instance_states
        .iter()
        .map(|x| x.instance_id.clone())
        .collect();
    let elbs = group.load_balancer_names.clone();
    deregister_from_elbs(req, &elbs, &ids);
    group.load_balancer_names.retain(|n| !names.contains(n));
    Ok(String::new())
}

fn op_attach_load_balancer_target_groups(
    req: &Request,
    state: &mut AsgState,
    p: &Params,
) -> OpResult<String> {
    let as_name = p_string(p, "AutoScalingGroupName")
        .unwrap_or_default()
        .to_string();
    let arns = p_list(p, "TargetGroupARNs");
    let Some(group) = state.autoscaling_groups.get_mut(&as_name) else {
        return Err(validation("AutoScalingGroup name not found - null"));
    };
    for arn in arns {
        if !group.target_group_arns.contains(&arn) {
            group.target_group_arns.push(arn);
        }
    }
    sync_target_groups(req, group);
    Ok(String::new())
}

fn op_describe_load_balancer_target_groups(state: &AsgState, p: &Params) -> OpResult<String> {
    let as_name = p_string(p, "AutoScalingGroupName")
        .unwrap_or_default()
        .to_string();
    let Some(group) = state.autoscaling_groups.get(&as_name) else {
        return Err(validation("AutoScalingGroup name not found - null"));
    };
    let members: Vec<String> = group
        .target_group_arns
        .iter()
        .map(|a| {
            format!(
                "{}{}",
                tag_esc("LoadBalancerTargetGroupARN", a),
                tag_esc("State", "Added")
            )
        })
        .collect();
    Ok(list_xml("LoadBalancerTargetGroups", &members))
}

fn op_detach_load_balancer_target_groups(
    req: &Request,
    state: &mut AsgState,
    p: &Params,
) -> OpResult<String> {
    let as_name = p_string(p, "AutoScalingGroupName")
        .unwrap_or_default()
        .to_string();
    let arns = p_list(p, "TargetGroupARNs");
    let Some(group) = state.autoscaling_groups.get_mut(&as_name) else {
        return Err(validation("AutoScalingGroup name not found - null"));
    };
    group.target_group_arns.retain(|a| !arns.contains(a));
    deny_deregister_target_groups(req, group, &arns);
    Ok(String::new())
}

// ---------------------------------------------------------------------------
// Notification configurations.
// ---------------------------------------------------------------------------

fn op_put_notification_configuration(state: &mut AsgState, p: &Params) -> OpResult<String> {
    let as_name = p_string(p, "AutoScalingGroupName")
        .unwrap_or_default()
        .to_string();
    let topic_arn = p_string(p, "TopicARN").unwrap_or_default().to_string();
    let types = p_list(p, "NotificationTypes");
    if !state.autoscaling_groups.contains_key(&as_name) {
        return Err(validation("AutoScalingGroup name not found - null"));
    }
    state
        .notification_configurations
        .retain(|nc| !(nc.auto_scaling_group_name == as_name && nc.topic_arn == topic_arn));
    state.notification_configurations.push(NotificationConfig {
        auto_scaling_group_name: as_name,
        topic_arn,
        notification_types: types,
    });
    Ok(String::new())
}

fn op_describe_notification_configurations(state: &AsgState, p: &Params) -> OpResult<String> {
    let names: Vec<String> = p_list(p, "AutoScalingGroupNames");
    let mut members = Vec::new();
    for nc in &state.notification_configurations {
        if !names.is_empty() && !names.contains(&nc.auto_scaling_group_name) {
            continue;
        }
        for nt in &nc.notification_types {
            members.push(format!(
                "{}{}{}",
                tag_esc("AutoScalingGroupName", &nc.auto_scaling_group_name),
                tag_esc("TopicARN", &nc.topic_arn),
                tag_esc("NotificationType", nt),
            ));
        }
    }
    Ok(list_xml("NotificationConfigurations", &members))
}

fn op_delete_notification_configuration(state: &mut AsgState, p: &Params) -> OpResult<String> {
    let as_name = p_string(p, "AutoScalingGroupName")
        .unwrap_or_default()
        .to_string();
    let topic_arn = p_string(p, "TopicARN").unwrap_or_default().to_string();
    state
        .notification_configurations
        .retain(|nc| !(nc.auto_scaling_group_name == as_name && nc.topic_arn == topic_arn));
    Ok(String::new())
}

// ---------------------------------------------------------------------------
// Instance refreshes.
// ---------------------------------------------------------------------------

fn op_start_instance_refresh(state: &mut AsgState, p: &Params) -> OpResult<String> {
    let as_name = p_string(p, "AutoScalingGroupName")
        .unwrap_or_default()
        .to_string();
    if !state.autoscaling_groups.contains_key(&as_name) {
        return Err(group_not_found(&as_name));
    }
    let in_flight = state
        .instance_refreshes
        .get(&as_name)
        .map(|list| {
            list.iter()
                .any(|r| r.status == "Pending" || r.status == "InProgress")
        })
        .unwrap_or(false);
    if in_flight {
        return Err(resource_contention());
    }
    let refresh = InstanceRefresh {
        instance_refresh_id: uuid::Uuid::new_v4().to_string(),
        auto_scaling_group_name: as_name.clone(),
        status: "Pending".to_string(),
        start_time: now_wire_ts(),
        end_time: None,
        preferences: p
            .get("Preferences")
            .and_then(raw_map_ref)
            .filter(|m| !m.is_empty())
            .cloned(),
    };
    let id = refresh.instance_refresh_id.clone();
    state
        .instance_refreshes
        .entry(as_name)
        .or_default()
        .push(refresh);
    Ok(tag_esc("InstanceRefreshId", &id))
}

fn op_describe_instance_refreshes(state: &AsgState, p: &Params) -> OpResult<String> {
    let as_name = p_string(p, "AutoScalingGroupName")
        .unwrap_or_default()
        .to_string();
    if !state.autoscaling_groups.contains_key(&as_name) {
        return Err(group_not_found(&as_name));
    }
    let ids: Vec<String> = p_list(p, "InstanceRefreshIds");
    let members: Vec<String> = state
        .instance_refreshes
        .get(&as_name)
        .map(|list| list.as_slice())
        .unwrap_or(&[])
        .iter()
        .filter(|r| ids.is_empty() || ids.contains(&r.instance_refresh_id))
        .map(refresh_xml_public)
        .collect();
    Ok(list_xml("InstanceRefreshes", &members))
}

fn refresh_xml_public(r: &InstanceRefresh) -> String {
    refresh_xml(r)
}

fn op_cancel_instance_refresh(state: &mut AsgState, p: &Params) -> OpResult<String> {
    let as_name = p_string(p, "AutoScalingGroupName")
        .unwrap_or_default()
        .to_string();
    if !state.autoscaling_groups.contains_key(&as_name) {
        return Err(group_not_found(&as_name));
    }
    let Some(list) = state.instance_refreshes.get_mut(&as_name) else {
        return Err(resource_contention());
    };
    for refresh in list.iter_mut() {
        if refresh.status == "Pending" || refresh.status == "InProgress" {
            refresh.status = "Cancelling".to_string();
            refresh.end_time = Some(now_wire_ts());
            return Ok(tag_esc("InstanceRefreshId", &refresh.instance_refresh_id));
        }
    }
    Err(resource_contention())
}

fn op_rollback_instance_refresh(state: &mut AsgState, p: &Params) -> OpResult<String> {
    let as_name = p_string(p, "AutoScalingGroupName")
        .unwrap_or_default()
        .to_string();
    if !state.autoscaling_groups.contains_key(&as_name) {
        return Err(group_not_found(&as_name));
    }
    let Some(list) = state.instance_refreshes.get_mut(&as_name) else {
        return Err(resource_contention());
    };
    for refresh in list.iter_mut() {
        if refresh.status == "InProgress" || refresh.status == "Pending" {
            refresh.status = "RollbackInProgress".to_string();
            return Ok(tag_esc("InstanceRefreshId", &refresh.instance_refresh_id));
        }
    }
    Err(resource_contention())
}

// ---------------------------------------------------------------------------
// Static catalog + stub operations (moto's answer values).
// ---------------------------------------------------------------------------

fn op_describe_account_limits(state: &AsgState) -> String {
    format!(
        "{}{}{}{}",
        tag_esc("MaxNumberOfAutoScalingGroups", "200"),
        tag_esc("MaxNumberOfLaunchConfigurations", "200"),
        tag_esc(
            "NumberOfAutoScalingGroups",
            &state.autoscaling_groups.len().to_string()
        ),
        tag_esc(
            "NumberOfLaunchConfigurations",
            &state.launch_configurations.len().to_string()
        ),
    )
}

fn op_describe_adjustment_types() -> String {
    let members: Vec<String> = [
        "ChangeInCapacity",
        "ExactCapacity",
        "PercentChangeInCapacity",
    ]
    .iter()
    .map(|t| tag_esc("AdjustmentType", t))
    .collect();
    list_xml("AdjustmentTypes", &members)
}

fn op_describe_auto_scaling_notification_types() -> String {
    let types = [
        "autoscaling:EC2_INSTANCE_LAUNCH",
        "autoscaling:EC2_INSTANCE_LAUNCH_ERROR",
        "autoscaling:EC2_INSTANCE_TERMINATE",
        "autoscaling:EC2_INSTANCE_TERMINATE_ERROR",
        "autoscaling:TEST_NOTIFICATION",
    ];
    list_xml(
        "AutoScalingNotificationTypes",
        &types.iter().map(|t| esc(t)).collect::<Vec<_>>(),
    )
}

fn op_describe_lifecycle_hook_types() -> String {
    list_xml(
        "LifecycleHookTypes",
        &[
            "autoscaling:EC2_INSTANCE_LAUNCHING",
            "autoscaling:EC2_INSTANCE_TERMINATING",
        ]
        .iter()
        .map(|t| esc(t))
        .collect::<Vec<_>>(),
    )
}

fn op_describe_scaling_process_types() -> String {
    let members: Vec<String> = PROCESS_NAMES
        .iter()
        .map(|p| tag_esc("ProcessName", p))
        .collect();
    list_xml("Processes", &members)
}

fn op_describe_termination_policy_types() -> String {
    list_xml(
        "TerminationPolicyTypes",
        &[
            "AllocationStrategy",
            "ClosestToNextInstanceHour",
            "Default",
            "NewestInstance",
            "OldestInstance",
            "OldestLaunchConfiguration",
            "OldestLaunchTemplate",
        ]
        .iter()
        .map(|t| esc(t))
        .collect::<Vec<_>>(),
    )
}

fn op_describe_metric_collection_types() -> String {
    let metric_names = [
        "GroupMinSize",
        "GroupMaxSize",
        "GroupDesiredCapacity",
        "GroupInServiceInstances",
        "GroupPendingInstances",
        "GroupStandbyInstances",
        "GroupTerminatingInstances",
        "GroupTotalInstances",
        "GroupInServiceCapacity",
        "GroupPendingCapacity",
        "GroupStandbyCapacity",
        "GroupTerminatingCapacity",
        "GroupTotalCapacity",
        "WarmPoolDesiredCapacity",
        "WarmPoolWarmedCapacity",
        "WarmPoolPendingCapacity",
        "WarmPoolTerminatingCapacity",
        "WarmPoolTotalCapacity",
        "GroupAndWarmPoolDesiredCapacity",
        "GroupAndWarmPoolTotalCapacity",
    ];
    let metrics: Vec<String> = metric_names.iter().map(|m| tag_esc("Metric", m)).collect();
    let granularities = list_xml(
        "Granularities",
        &[format!(
            "<member>{}{}</member>",
            tag_esc("Granularity", "1Minute"),
            ""
        )],
    );
    format!("{}{}", list_xml("Metrics", &metrics), granularities)
}

fn op_describe_scaling_activities() -> String {
    // moto's stub never records activities
    "<Activities/>".to_string()
}

fn op_get_predictive_scaling_forecast(state: &AsgState, p: &Params) -> OpResult<String> {
    let name = p_string(p, "AutoScalingGroupName")
        .unwrap_or_default()
        .to_string();
    if !state.autoscaling_groups.contains_key(&name) {
        return Err(group_not_found(&name));
    }
    Ok(op_get_predictive_scaling_forecast_body())
}

fn op_get_predictive_scaling_forecast_body() -> String {
    format!(
        "<LoadForecast/>{}{}",
        tag_raw("CapacityForecast", "<Timestamps/><Values/>"),
        tag_esc("UpdateTime", &now_wire_ts()),
    )
}

// ---------------------------------------------------------------------------
// Dispatch.
// ---------------------------------------------------------------------------

/// Route an Auto Scaling query request to the native backend.
pub fn dispatch(req: &Request) -> Option<Response> {
    let op = action_of(req)?;
    let p = parse_params(req);
    match run_op(req, p, &op) {
        Ok(body) => Some(result_response(&op, &body)),
        Err(err) => Some(error_response(&err)),
    }
}

fn run_op(req: &Request, p: Params, op: &str) -> Result<String, AsgError> {
    let (region, account, op_string) = (req.region.clone(), req.account, op.to_string());
    with_state(req, move |state| {
        route_op(req, state, &p, &op_string, &region, account)
    })
}

/// The op table with the state lease already held.
fn route_op(
    req: &Request,
    state: &mut AsgState,
    p: &Params,
    op: &str,
    region: &str,
    account: u64,
) -> Result<String, AsgError> {
    match op {
        "CreateLaunchConfiguration" => op_create_launch_configuration(state, p, account, region),
        "DescribeLaunchConfigurations" => op_describe_launch_configurations(state, p),
        "DeleteLaunchConfiguration" => op_delete_launch_configuration(state, p),
        "CreateAutoScalingGroup" => op_create_auto_scaling_group(req, state, p, account, region),
        "UpdateAutoScalingGroup" => op_update_auto_scaling_group(req, state, p),
        "DeleteAutoScalingGroup" => op_delete_auto_scaling_group(req, state, p),
        "DescribeAutoScalingGroups" => op_describe_auto_scaling_groups(state, p),
        "SetDesiredCapacity" => op_set_desired_capacity(req, state, p),
        "DescribeAutoScalingInstances" => op_describe_auto_scaling_instances(state, p),
        "AttachInstances" => op_attach_instances(req, state, p),
        "DetachInstances" => op_detach_instances(req, state, p),
        "SetInstanceHealth" => op_set_instance_health(req, state, p),
        "EnterStandby" => op_enter_standby(req, state, p),
        "ExitStandby" => op_exit_standby(req, state, p),
        "TerminateInstanceInAutoScalingGroup" => op_terminate_instance(req, state, p),
        "SuspendProcesses" => op_suspend_processes(state, p),
        "ResumeProcesses" => op_resume_processes(state, p),
        "SetInstanceProtection" => op_set_instance_protection(state, p),
        "EnableMetricsCollection" => op_enable_metrics_collection(state, p),
        "DisableMetricsCollection" => op_disable_metrics_collection(state, p),
        "PutWarmPool" => op_put_warm_pool(state, p),
        "DescribeWarmPool" => op_describe_warm_pool(state, p),
        "DeleteWarmPool" => op_delete_warm_pool(state, p),
        "PutLifecycleHook" => op_put_lifecycle_hook(state, p),
        "DescribeLifecycleHooks" => op_describe_lifecycle_hooks(state, p),
        "DeleteLifecycleHook" => op_delete_lifecycle_hook(state, p),
        "CompleteLifecycleAction" => op_complete_lifecycle_action(state, p),
        "RecordLifecycleActionHeartbeat" => op_record_lifecycle_action_heartbeat(state, p),
        "PutScalingPolicy" => op_put_scaling_policy(state, p, account, region),
        "DescribePolicies" => op_describe_policies(state, p, region, account),
        "DeletePolicy" => op_delete_policy(state, p),
        "ExecutePolicy" => op_execute_policy(req, state, p),
        "PutScheduledUpdateGroupAction" => op_put_scheduled_update_group_action(state, p),
        "BatchPutScheduledUpdateGroupAction" => {
            op_batch_put_scheduled_update_group_action(state, p)
        }
        "DeleteScheduledAction" => op_delete_scheduled_action(state, p),
        "BatchDeleteScheduledAction" => op_batch_delete_scheduled_action(state, p),
        "DescribeScheduledActions" => op_describe_scheduled_actions(state, p),
        "AttachTrafficSources" => op_attach_traffic_sources(state, p),
        "DescribeTrafficSources" => op_describe_traffic_sources(state, p),
        "DetachTrafficSources" => op_detach_traffic_sources(state, p),
        "AttachLoadBalancers" => op_attach_load_balancers(req, state, p),
        "DescribeLoadBalancers" => op_describe_load_balancers(state, p),
        "DetachLoadBalancers" => op_detach_load_balancers(req, state, p),
        "AttachLoadBalancerTargetGroups" => op_attach_load_balancer_target_groups(req, state, p),
        "DescribeLoadBalancerTargetGroups" => op_describe_load_balancer_target_groups(state, p),
        "DetachLoadBalancerTargetGroups" => op_detach_load_balancer_target_groups(req, state, p),
        "PutNotificationConfiguration" => op_put_notification_configuration(state, p),
        "DescribeNotificationConfigurations" => op_describe_notification_configurations(state, p),
        "DeleteNotificationConfiguration" => op_delete_notification_configuration(state, p),
        "StartInstanceRefresh" => op_start_instance_refresh(state, p),
        "DescribeInstanceRefreshes" => op_describe_instance_refreshes(state, p),
        "CancelInstanceRefresh" => op_cancel_instance_refresh(state, p),
        "RollbackInstanceRefresh" => op_rollback_instance_refresh(state, p),
        "CreateOrUpdateTags" => op_create_or_update_tags(state, p),
        "DeleteTags" => op_delete_tags(state, p),
        "DescribeTags" => op_describe_tags(state, p),
        "DescribeAccountLimits" => Ok(op_describe_account_limits(state)),
        "DescribeAdjustmentTypes" => Ok(op_describe_adjustment_types()),
        "DescribeAutoScalingNotificationTypes" => Ok(op_describe_auto_scaling_notification_types()),
        "DescribeLifecycleHookTypes" => Ok(op_describe_lifecycle_hook_types()),
        "DescribeScalingProcessTypes" => Ok(op_describe_scaling_process_types()),
        "DescribeTerminationPolicyTypes" => Ok(op_describe_termination_policy_types()),
        "DescribeMetricCollectionTypes" => Ok(op_describe_metric_collection_types()),
        "DescribeScalingActivities" => Ok(op_describe_scaling_activities()),
        "GetPredictiveScalingForecast" => op_get_predictive_scaling_forecast(state, p),
        other => Err(not_implemented(other)),
    }
}

// ---------------------------------------------------------------------------
// Unit tests (moto-era semantics spot checks, no server required).
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn req_for(op: &str, rest: &str) -> Request {
        Request {
            service: "autoscaling".into(),
            operation: op.into(),
            method: "POST".into(),
            path: "/".into(),
            query_string: String::new(),
            headers: Default::default(),
            body: bytes::Bytes::from(format!("Action={op}&Version=2011-01-01&{rest}")),
            account: 123456789012,
            region: "us-east-1".into(),
        }
    }

    /// Run `req` against a persistent per-account state; returns the result XML
    /// body or the modeled error (loopback calls answer None in unit tests, so
    /// launches stay empty).
    fn run_again(acct: u64, req: &Request) -> Result<String, AsgError> {
        // Re-key the request so each test drives its own (account, region)
        // scope despite req_for's fixed account.
        let scoped = Request {
            account: acct,
            ..req.clone()
        };
        let op = scoped.operation.clone();
        let p = parse_params(&scoped);
        let region = scoped.region.clone();
        let scoped_ref = &scoped;
        with_state(&scoped, move |state| {
            route_op(scoped_ref, state, &p, &op, &region, acct)
        })
    }

    fn with_state<T>(req: &Request, f: impl FnOnce(&mut AsgState) -> T) -> T {
        let slots = store::slot::<AsgState>("autoscaling");
        let mut map = slots.write();
        let state = map.entry((req.account, req.region.clone())).or_default();
        f(state)
    }

    #[test]
    fn launch_configuration_roundtrip_and_validation() {
        let req = req_for(
            "CreateLaunchConfiguration",
            "LaunchConfigurationName=lc1&ImageId=ami-12345678&InstanceType=t2.micro&KeyName=kp",
        );
        let body = run_again(11, &req).expect("create lc");
        assert_eq!(body, "");
        let body = run_again(
            11,
            &req_for(
                "DescribeLaunchConfigurations",
                "LaunchConfigurationNames.member.1=lc1",
            ),
        )
        .expect("describe");
        assert!(body.contains("<LaunchConfigurationName>lc1</LaunchConfigurationName>"));
        assert!(body.contains("<InstanceType>t2.micro</InstanceType>"));
        assert!(body.contains(
            "launchConfiguration:9dbbbf87-6141-428a-a409-0752edbe6cad:launchConfigurationName/lc1"
        ));
        assert!(body.contains("<InstanceMonitoring><Enabled>false</Enabled></InstanceMonitoring>"));
        assert!(body.contains("<EbsOptimized>false</EbsOptimized>"));
        // missing ImageId/InstanceType is a ValidationError
        let err = run_again(
            11,
            &req_for("CreateLaunchConfiguration", "LaunchConfigurationName=lc2"),
        )
        .unwrap_err();
        assert_eq!(err.code, "ValidationError");
        // delete clears it
        run_again(
            11,
            &req_for("DeleteLaunchConfiguration", "LaunchConfigurationName=lc1"),
        )
        .expect("delete");
        let body = run_again(
            11,
            &req_for(
                "DescribeLaunchConfigurations",
                "LaunchConfigurationNames.member.1=lc1",
            ),
        )
        .expect("describe");
        assert_eq!(body, "<LaunchConfigurations/>");
    }

    #[test]
    fn group_roundtrip_and_bounds() {
        run_again(
            12,
            &req_for(
                "CreateLaunchConfiguration",
                "LaunchConfigurationName=lc1&ImageId=ami-1&InstanceType=t2.micro",
            ),
        )
        .expect("lc");
        let create = req_for(
            "CreateAutoScalingGroup",
            "AutoScalingGroupName=g1&LaunchConfigurationName=lc1&MinSize=0&MaxSize=2&AvailabilityZones.member.1=us-east-1a",
        );
        run_again(12, &create).expect("create asg");
        let body = run_again(
            12,
            &req_for(
                "DescribeAutoScalingGroups",
                "AutoScalingGroupNames.member.1=g1",
            ),
        )
        .expect("describe");
        assert!(body.contains("<AutoScalingGroupName>g1</AutoScalingGroupName>"));
        assert!(body.contains("autoScalingGroup:"));
        assert!(body.contains("<MinSize>0</MinSize>"));
        assert!(body.contains("<MaxSize>2</MaxSize>"));
        assert!(body.contains("<HealthCheckType>EC2</HealthCheckType>"));
        assert!(
            body.contains("<TerminationPolicies><member>Default</member></TerminationPolicies>")
        );
        // duplicate launch params reject
        let err = run_again(
            12,
            &req_for(
                "CreateAutoScalingGroup",
                "AutoScalingGroupName=g2&LaunchConfigurationName=lc1&InstanceType=t2.micro&MinSize=0&MaxSize=1",
            ),
        )
        .unwrap_err();
        assert_eq!(err.code, "ValidationError");
        // without any AZ / subnet rejects
        let err = run_again(
            12,
            &req_for(
                "CreateAutoScalingGroup",
                "AutoScalingGroupName=g3&LaunchConfigurationName=lc1&MinSize=0&MaxSize=1",
            ),
        )
        .unwrap_err();
        assert_eq!(err.code, "ValidationError");
        // deleting an unknown group is a ValidationError (moto crashed with 500)
        let err = run_again(
            12,
            &req_for("DeleteAutoScalingGroup", "AutoScalingGroupName=nope"),
        )
        .unwrap_err();
        assert_eq!(err.code, "ValidationError");
    }

    #[test]
    fn tags_roundtrip_and_filters() {
        run_again(
            13,
            &req_for(
                "CreateLaunchConfiguration",
                "LaunchConfigurationName=lc&ImageId=ami-1&InstanceType=t2.micro",
            ),
        )
        .expect("lc");
        run_again(
            13,
            &req_for(
                "CreateAutoScalingGroup",
                "AutoScalingGroupName=asg&LaunchConfigurationName=lc&MinSize=0&MaxSize=1&AvailabilityZones.member.1=us-east-1a",
            ),
        )
        .expect("asg");
        run_again(
            13,
            &req_for(
                "CreateOrUpdateTags",
                "Tags.member.1.ResourceId=asg&Tags.member.1.ResourceType=auto-scaling-group&\
                 Tags.member.1.Key=Env&Tags.member.1.Value=prod&Tags.member.1.PropagateAtLaunch=true",
            ),
        )
        .expect("tag");
        let body = run_again(
            13,
            &req_for(
                "DescribeTags",
                "Filters.member.1.Name=auto-scaling-group&Filters.member.1.Values.member.1=asg",
            ),
        )
        .expect("describe tags");
        assert!(body.contains("<Key>Env</Key>"));
        assert!(body.contains("<ResourceId>asg</ResourceId>"));
        assert!(body.contains("<PropagateAtLaunch>true</PropagateAtLaunch>"));
        // deleting the tag removes it
        run_again(
            13,
            &req_for(
                "DeleteTags",
                "Tags.member.1.ResourceId=asg&Tags.member.1.Key=Env",
            ),
        )
        .expect("delete tags");
        let body = run_again(
            13,
            &req_for(
                "DescribeTags",
                "Filters.member.1.Name=auto-scaling-group&Filters.member.1.Values.member.1=asg",
            ),
        )
        .expect("describe tags");
        assert!(!body.contains("Env"));
    }

    #[test]
    fn suspension_and_metrics_collection() {
        seed_group(14, "asg");
        run_again(
            14,
            &req_for(
                "SuspendProcesses",
                "AutoScalingGroupName=asg&ScalingProcesses.member.1=Launch&ScalingProcesses.member.2=Terminate",
            ),
        )
        .expect("suspend");
        let body = run_again(
            14,
            &req_for(
                "DescribeAutoScalingGroups",
                "AutoScalingGroupNames.member.1=asg",
            ),
        )
        .expect("describe");
        assert!(body.contains("<ProcessName>Launch</ProcessName>"));
        run_again(
            14,
            &req_for(
                "ResumeProcesses",
                "AutoScalingGroupName=asg&ScalingProcesses.member.1=Launch",
            ),
        )
        .expect("resume");
        let body = run_again(
            14,
            &req_for(
                "DescribeAutoScalingGroups",
                "AutoScalingGroupNames.member.1=asg",
            ),
        )
        .expect("describe");
        assert!(!body.contains("<ProcessName>Launch</ProcessName>"));
        // unknown group is a modeled ValidationError (moto crashed there)
        let err = run_again(
            14,
            &req_for("DisableMetricsCollection", "AutoScalingGroupName=nope"),
        )
        .unwrap_err();
        assert_eq!(err.code, "ValidationError");
    }

    #[test]
    fn policy_roundtrip_and_execute() {
        seed_group(15, "asg");
        let body = run_again(
            15,
            &req_for(
                "PutScalingPolicy",
                "AutoScalingGroupName=asg&PolicyName=pol&PolicyType=SimpleScaling&\
                 AdjustmentType=ChangeInCapacity&ScalingAdjustment=2&Cooldown=60",
            ),
        )
        .expect("put policy");
        assert!(body.contains("scalingPolicy:c322761b-3172-4d56-9a21-0ed9d6161d67"));
        let body = run_again(15, &req_for("DescribePolicies", "AutoScalingGroupName=asg"))
            .expect("describe policies");
        assert!(body.contains("<PolicyName>pol</PolicyName>"));
        assert!(body.contains("<ScalingAdjustment>2</ScalingAdjustment>"));
        assert!(body.contains("<Cooldown>60</Cooldown>"));
        // execute bumps the desired capacity (no live EC2 in unit tests)
        run_again(
            15,
            &req_for(
                "ExecutePolicy",
                "AutoScalingGroupName=asg&PolicyName=pol&HonorCooldown=false",
            ),
        )
        .expect("execute");
        let body = run_again(
            15,
            &req_for(
                "DescribeAutoScalingGroups",
                "AutoScalingGroupNames.member.1=asg",
            ),
        )
        .expect("describe");
        assert!(body.contains("<DesiredCapacity>2</DesiredCapacity>"));
    }

    #[test]
    fn warm_pool_and_instance_refresh_lifecycle() {
        seed_group(16, "asg");
        run_again(
            16,
            &req_for(
                "PutWarmPool",
                "AutoScalingGroupName=asg&MaxGroupPreparedCapacity=2",
            ),
        )
        .expect("put warm pool");
        let body = run_again(16, &req_for("DescribeWarmPool", "AutoScalingGroupName=asg"))
            .expect("describe warm pool");
        assert!(body.contains("<PoolState>Stopped</PoolState>"));
        assert!(body.contains("<MaxGroupPreparedCapacity>2</MaxGroupPreparedCapacity>"));
        run_again(
            16,
            &req_for(
                "StartInstanceRefresh",
                "AutoScalingGroupName=asg&Strategy=Rolling",
            ),
        )
        .expect("start refresh");
        let err = run_again(
            16,
            &req_for("StartInstanceRefresh", "AutoScalingGroupName=asg"),
        )
        .unwrap_err();
        assert_eq!(err.code, "ResourceContention");
        assert!(
            err.doubled_message,
            "unmodeled ResourceContention duplicates <message>"
        );
        run_again(
            16,
            &req_for("CancelInstanceRefresh", "AutoScalingGroupName=asg"),
        )
        .expect("cancel");
        let body = run_again(
            16,
            &req_for("DescribeInstanceRefreshes", "AutoScalingGroupName=asg"),
        )
        .expect("describe refreshes");
        assert!(body.contains("<Status>Cancelling</Status>"));
        // nothing in flight anymore → ResourceContention
        let err = run_again(
            16,
            &req_for("CancelInstanceRefresh", "AutoScalingGroupName=asg"),
        )
        .unwrap_err();
        assert_eq!(err.code, "ResourceContention");
        // unknown group → ValidationError
        let err = run_again(
            16,
            &req_for("RollbackInstanceRefresh", "AutoScalingGroupName=nope"),
        )
        .unwrap_err();
        assert_eq!(err.code, "ValidationError");
    }

    #[test]
    fn traffic_sources_and_lifecycle_hooks() {
        seed_group(17, "asg");
        run_again(
            17,
            &req_for(
                "AttachTrafficSources",
                "AutoScalingGroupName=asg&TrafficSources.member.1.Identifier=arn:aws:t1&TrafficSources.member.1.Type=elbv2",
            ),
        )
        .expect("attach");
        let body = run_again(
            17,
            &req_for("DescribeTrafficSources", "AutoScalingGroupName=asg"),
        )
        .expect("describe");
        assert!(body.contains("<Identifier>arn:aws:t1</Identifier>"));
        assert!(body.contains("<State>InService</State>"));
        let err = run_again(
            17,
            &req_for("DescribeTrafficSources", "AutoScalingGroupName=nope"),
        )
        .unwrap_err();
        assert_eq!(err.code, "ValidationError");
        run_again(
            17,
            &req_for(
                "DetachTrafficSources",
                "AutoScalingGroupName=asg&TrafficSources.member.1.Identifier=arn:aws:t1",
            ),
        )
        .expect("detach");
        let body = run_again(
            17,
            &req_for("DescribeTrafficSources", "AutoScalingGroupName=asg"),
        )
        .expect("describe");
        assert_eq!(body, "<TrafficSources/>");

        // hooks
        run_again(
            17,
            &req_for(
                "PutLifecycleHook",
                "LifecycleHookName=h1&AutoScalingGroupName=asg&LifecycleTransition=autoscaling:EC2_INSTANCE_LAUNCHING&HeartbeatTimeout=123",
            ),
        )
        .expect("put hook");
        let body = run_again(
            17,
            &req_for("DescribeLifecycleHooks", "AutoScalingGroupName=asg"),
        )
        .expect("describe hooks");
        assert!(body.contains("<LifecycleHookName>h1</LifecycleHookName>"));
        assert!(body.contains("<HeartbeatTimeout>123</HeartbeatTimeout>"));
        assert!(body.contains("<GlobalTimeout>172800</GlobalTimeout>"));
        // complete against a missing hook is a ValidationError
        let err = run_again(
            17,
            &req_for(
                "CompleteLifecycleAction",
                "LifecycleHookName=ghost&AutoScalingGroupName=asg&LifecycleActionResult=CONTINUE",
            ),
        )
        .unwrap_err();
        assert_eq!(err.code, "ValidationError");
    }

    #[test]
    fn launch_instances_falls_through_not_implemented() {
        let err = run_again(
            19,
            &req_for(
                "LaunchInstances",
                "AutoScalingGroupName=fake-asg&RequestedCapacity=1",
            ),
        )
        .unwrap_err();
        assert_eq!(err.status, 501);
        assert_eq!(err.code, "NotImplemented");
        assert_eq!(
            err.message,
            "The LaunchInstances action has not been implemented"
        );
    }

    fn seed_group(acct: u64, name: &str) {
        run_again(
            acct,
            &req_for(
                "CreateLaunchConfiguration",
                "LaunchConfigurationName=lc&ImageId=ami-1&InstanceType=t2.micro",
            ),
        )
        .expect("seed lc");
        run_again(
            acct,
            &req_for(
                "CreateAutoScalingGroup",
                &format!(
                    "AutoScalingGroupName={name}&LaunchConfigurationName=lc&MinSize=0&MaxSize=3&\
                     AvailabilityZones.member.1=us-east-1a"
                ),
            ),
        )
        .expect("seed asg");
    }

    #[test]
    fn cloudtrail_emit_fires_only_on_successful_group_calls() {
        // Registry claim contract (crate::cloudtrail_emit::test_claim +
        // reset_registry): hold the claim guard for the whole
        // reset/spy/restore window, restore the unbound default at the end.
        // Dedicated account scope, isolated from the tests above.
        let _claim = crate::cloudtrail_emit::test_claim();
        crate::cloudtrail_emit::reset_registry();
        let seen: std::sync::Arc<parking_lot::Mutex<Vec<serde_json::Value>>> =
            std::sync::Arc::new(parking_lot::Mutex::new(Vec::new()));
        {
            let sink = seen.clone();
            crate::cloudtrail_emit::register_emitter(std::sync::Arc::new(
                move |account, region, source, event, request| {
                    // Only this test's scope (21): other tests drive the same
                    // group CRUD concurrently against their own accounts.
                    if account != 21 {
                        return;
                    }
                    sink.lock().push(serde_json::json!({
                        "account": account,
                        "region": region,
                        "source": source,
                        "event": event,
                        "request": serde_json::from_str::<serde_json::Value>(request)
                            .unwrap_or_default(),
                    }));
                },
            ));
        }
        run_again(
            21,
            &req_for(
                "CreateLaunchConfiguration",
                "LaunchConfigurationName=ct-lc&ImageId=ami-1&InstanceType=t2.micro",
            ),
        )
        .expect("lc"); // launch configs are not on the emit allowlist
        run_again(
            21,
            &req_for(
                "CreateAutoScalingGroup",
                "AutoScalingGroupName=ct-g&LaunchConfigurationName=ct-lc&MinSize=0&MaxSize=2\
                 &AvailabilityZones.member.1=us-east-1a&DesiredCapacity=0",
            ),
        )
        .expect("create asg");
        // A failed delete emits nothing (no such group).
        assert!(run_again(
            21,
            &req_for("DeleteAutoScalingGroup", "AutoScalingGroupName=missing")
        )
        .is_err());
        run_again(
            21,
            &req_for(
                "DeleteAutoScalingGroup",
                "AutoScalingGroupName=ct-g&ForceDelete=true",
            ),
        )
        .expect("delete asg");
        crate::cloudtrail_emit::reset_registry();

        let rows = seen.lock().clone();
        assert_eq!(rows.len(), 2, "only the two successful group calls emit");
        assert_eq!(rows[0]["source"], "autoscaling.amazonaws.com");
        assert_eq!(rows[0]["event"], "CreateAutoScalingGroup");
        assert_eq!(rows[0]["account"], serde_json::json!(21));
        assert_eq!(rows[0]["region"], "us-east-1");
        assert_eq!(rows[0]["request"]["autoScalingGroupName"], "ct-g");
        assert_eq!(rows[0]["request"]["maxSize"], serde_json::json!(2));
        assert_eq!(
            rows[0]["request"]["availabilityZones"][0], "us-east-1a",
            "numeric members stay numeric and lists stay lists"
        );
        assert_eq!(rows[0]["request"]["launchConfigurationName"], "ct-lc");
        assert!(rows[0]["request"].get("forceDelete").is_none());
        assert_eq!(rows[1]["event"], "DeleteAutoScalingGroup");
        assert_eq!(rows[1]["request"]["autoScalingGroupName"], "ct-g");
        assert_eq!(
            rows[1]["request"]["forceDelete"],
            serde_json::json!(true),
            "fields the call carried are echoed, absent ones are not"
        );
    }

    #[test]
    fn delete_group_with_deleted_target_groups() {
        // moto master e70a7315 (#10412): target groups can be deleted before
        // their Auto Scaling group, and DeleteAutoScalingGroup must still
        // succeed — moto 5.2.4 tripped on set_desired_capacity's target
        // refresh and answered TargetGroupNotFound. The unit-test harness
        // has no loopback peer (no Host header, sync calls skip), which is
        // exactly the environment where the delete must not consult target
        // groups at all.
        for deleted_count in [0usize, 1, 2] {
            let acct = 2000u64 + deleted_count as u64;
            run_again(
                acct,
                &req_for(
                    "CreateLaunchConfiguration",
                    "LaunchConfigurationName=fx5lc&ImageId=ami-1&InstanceType=t2.micro",
                ),
            )
            .expect("launch config");
            let mut form =
                "AutoScalingGroupName=asg&LaunchConfigurationName=fx5lc&MinSize=0&MaxSize=1&\
                 AvailabilityZones.member.1=us-east-1a"
                    .to_string();
            for i in 0..2 {
                form.push_str(&format!(
                    "&TargetGroupARNs.member.{}=arn:aws:elasticloadbalancing:us-east-1:{}:targetgroup/fx5-{i}/abc{i}",
                    i + 1,
                    acct
                ));
            }
            run_again(acct, &req_for("CreateAutoScalingGroup", &form)).expect("create asg");
            // (deleted_count of those target groups may be deleted before
            // the ASG is deleted — the live twin probe covers that case
            // against a real elbv2 store.)
            run_again(
                acct,
                &req_for(
                    "DeleteAutoScalingGroup",
                    "AutoScalingGroupName=asg&ForceDelete=true",
                ),
            )
            .expect("delete must succeed even when target groups are deleted");
            let body = run_again(acct, &req_for("DescribeAutoScalingGroups", "")).unwrap();
            assert!(
                !body.contains("<AutoScalingGroupName>asg</AutoScalingGroupName>"),
                "deleted_count={deleted_count}, got {body}"
            );
        }
    }
}
