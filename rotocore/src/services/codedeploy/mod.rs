//! AWS CodeDeploy backend — port of moto's `codedeploy` engine (JSON 1.1,
//! target `CodeDeploy_20141006`).
//!
//! Ops arrive through the X-Amz-Target suffix; bodies keep the spec's
//! lowercase-first member names ("applicationName", "deploymentGroupName")
//! while the tag ops keep their PascalCase "ResourceArn"/"Tags"/"TagKeys".
//! Applications, per-application deployment groups, deployments, configs
//! and on-premises instances live per (account, region), mirroring moto's
//! in-memory backends — including its quirks: deployments bake their
//! application/group names at creation but keep live references to the
//! group and application for `deploymentStyle`/`computePlatform` (a group
//! survives its own deletion while a deployment still points at it), app
//! renames rekey the stored objects, revisions are stored under a canonical
//! serialization of the revision location, and the seventeen predefined
//! deployment config names exist implicitly and cannot be deleted.

use crate::proto::{json_error, now_iso, parse_json_body};
use crate::store;
use crate::{Request, Response};
use once_cell::sync::Lazy;
use serde_json::{json, Map, Value};
use std::collections::HashMap;

const DEFAULT_CONFIGS: [&str; 17] = [
    "CodeDeployDefault.AllAtOnce",
    "CodeDeployDefault.HalfAtATime",
    "CodeDeployDefault.OneAtATime",
    "CodeDeployDefault.ECSLinear10PercentEvery1Minutes",
    "CodeDeployDefault.ECSLinear10PercentEvery3Minutes",
    "CodeDeployDefault.ECSCanary10Percent5Minutes",
    "CodeDeployDefault.ECSCanary10Percent15Minutes",
    "CodeDeployDefault.ECSAllAtOnce",
    "CodeDeployDefault.LambdaLinear10PercentEvery1Minute",
    "CodeDeployDefault.LambdaLinear10PercentEvery2Minutes",
    "CodeDeployDefault.LambdaLinear10PercentEvery3Minutes",
    "CodeDeployDefault.LambdaLinear10PercentEvery10Minutes",
    "CodeDeployDefault.LambdaCanary10Percent5Minutes",
    "CodeDeployDefault.LambdaCanary10Percent10Minutes",
    "CodeDeployDefault.LambdaCanary10Percent15Minutes",
    "CodeDeployDefault.LambdaCanary10Percent30Minutes",
    "CodeDeployDefault.LambdaAllAtOnce",
];

fn state() -> store::Slot<State> {
    static SLOT: Lazy<store::Slot<State>> = Lazy::new(|| store::slot("codedeploy"));
    SLOT.clone()
}

fn ok(v: &Value) -> Response {
    Response::json(200, serde_json::to_vec(v).unwrap_or_default())
}

// --- Error constructors (moto's codedeploy/exceptions.py) --------------------

fn app_does_not_exist(name: &str) -> Response {
    json_error(
        400,
        "ApplicationDoesNotExistException",
        &format!("The application {name} does not exist with the user or AWS account."),
    )
}

fn app_already_exists(name: &str) -> Response {
    json_error(
        400,
        "ApplicationAlreadyExistsException",
        &format!("The application {name} already exists with the user or AWS account."),
    )
}

fn app_name_required() -> Response {
    json_error(
        400,
        "ApplicationNameRequiredException",
        "If deploymentGroupName is specified, applicationName must be specified.",
    )
}

fn deployment_does_not_exist(id: &str) -> Response {
    json_error(
        400,
        "DeploymentDoesNotExistException",
        &format!("The deployment {id} does not exist with the user or AWS account."),
    )
}

fn deployment_group_already_exists(name: &str) -> Response {
    json_error(
        400,
        "DeploymentGroupAlreadyExistsException",
        &format!("Deployment group {name} already exists."),
    )
}

fn deployment_group_name_required(msg: &str) -> Response {
    json_error(400, "DeploymentGroupNameRequiredException", msg)
}

fn deployment_group_does_not_exist(name: &str) -> Response {
    json_error(
        400,
        "DeploymentGroupDoesNotExistException",
        &format!("The deployment group {name} does not exist with the user or AWS account."),
    )
}

fn config_already_exists(name: &str) -> Response {
    json_error(
        400,
        "DeploymentConfigAlreadyExistsException",
        &format!("A deployment configuration with the name {name} already exists."),
    )
}

fn config_does_not_exist(name: &str) -> Response {
    json_error(
        400,
        "DeploymentConfigDoesNotExistException",
        &format!("The deployment config {name} does not exist."),
    )
}

fn config_in_use(msg: &str) -> Response {
    json_error(400, "DeploymentConfigInUseException", msg)
}

fn instance_name_required() -> Response {
    json_error(
        400,
        "InstanceNameRequiredException",
        "Instance name is required.",
    )
}

fn instance_not_registered(name: &str) -> Response {
    json_error(
        400,
        "InstanceNotRegisteredException",
        &format!("The on-premises instance {name} is not registered."),
    )
}

// --- State -------------------------------------------------------------------

#[derive(Default)]
struct State {
    /// app name -> application (insertion-ordered like moto's dict)
    applications: Vec<(String, Application)>,
    /// app name -> (group name -> group), moto's two-level dict
    deployment_groups: Vec<(String, Vec<(String, DeploymentGroup)>)>,
    /// deployment id -> deployment info (insertion-ordered)
    deployments: Vec<(String, DeploymentInfo)>,
    /// config name -> config (insertion-ordered)
    deployment_configs: Vec<(String, DeploymentConfig)>,
    /// instance name -> on-premises instance (insertion-ordered)
    on_premises: Vec<(String, OnPremisesInstance)>,
    /// moto's TaggingService: arn -> ordered {tag key: tag value}
    tags: Vec<(String, Map<String, Value>)>,
    /// Applications detached by DeleteApplication but still referenced by
    /// surviving deployments (moto keeps the Python objects alive through
    /// the reference chain — see delete_application's baked-name quirk).
    detached_apps: Vec<Application>,
    /// Deployment groups detached by a deletion while deployments still
    /// reference them (moto's DeploymentInfo holds the group object).
    detached_groups: Vec<DeploymentGroup>,
}

struct Application {
    id: String,
    name: String,
    /// raw spec value — null when CreateApplication omits computePlatform
    compute_platform: Value,
    create_time: String,
    /// canonical revision key -> GenericRevisionInfo
    revisions: HashMap<String, Value>,
}

struct DeploymentGroup {
    app_id: String,
    name: String,
    deployment_group_id: String,
    deployment_config_name: Option<String>,
    ec2_tag_filters: Value,
    on_premises_instance_tag_filters: Value,
    auto_scaling_groups: Value,
    service_role_arn: String,
    trigger_configurations: Value,
    auto_rollback_configuration: Value,
    outdated_instances_strategy: Option<String>,
    deployment_style: Value,
    blue_green_deployment_configuration: Value,
    load_balancer_info: Value,
    ec2_tag_set: Value,
    ecs_services: Value,
    on_premises_tag_set: Value,
    tags: Value,
    termination_hook_enabled: Value,
}

struct DeploymentInfo {
    application_name: String,
    deployment_group_name: String,
    deployment_config_name: Option<String>,
    deployment_id: String,
    application_id: String,
    group_id: String,
    revision: Option<Value>,
    status: String,
    create_time: String,
    description: Option<String>,
    ignore_application_stop_failures: Option<Value>,
    target_instances: Option<Value>,
    auto_rollback_configuration: Option<Value>,
    update_outdated_instances_only: Option<Value>,
    file_exists_behavior: Option<String>,
    override_alarm_configuration: Option<Value>,
}

struct DeploymentConfig {
    deployment_config_id: String,
    deployment_config_name: String,
    minimum_healthy_hosts: Value,
    traffic_routing_config: Value,
    compute_platform: Value,
    zonal_config: Value,
    create_time: String,
}

struct OnPremisesInstance {
    instance_name: String,
    iam_session_arn: String,
    iam_user_arn: String,
    register_time: String,
    deregister_time: Option<String>,
    tags: Vec<Value>,
}

/// Python truthiness for values moto applies truth checks to.
fn truthy(v: &Value) -> bool {
    match v {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64().map(|f| f != 0.0).unwrap_or(false),
        Value::String(s) => !s.is_empty(),
        Value::Array(a) => !a.is_empty(),
        Value::Object(o) => !o.is_empty(),
    }
}

/// The `x or default` behavior: missing/null and falsy values are replaced.
fn param_or(v: Option<&Value>, default: Value) -> Value {
    match v {
        None | Some(Value::Null) => default,
        Some(v) if !truthy(v) => default,
        Some(v) => v.clone(),
    }
}

fn opt_str(p: &Value, key: &str) -> Option<String> {
    p.get(key).and_then(Value::as_str).map(String::from)
}

/// Canonical revision key for a RevisionLocation (moto hashes `str(dict)`;
/// the request wire order is canonical because botocore serializes members
/// in shape order, so a value-level serialization agrees both ways).
fn revision_key(revision: &Value) -> String {
    serde_json::to_string(revision).unwrap_or_else(|_| "()".to_string())
}

/// Render moto's `str(value)` for optional strings ("None" when absent).
fn py_str(v: &Option<String>) -> String {
    v.clone().unwrap_or_else(|| "None".to_string())
}

impl State {
    fn app(&self, name: &str) -> Option<&Application> {
        for (n, a) in &self.applications {
            if n == name {
                return Some(a);
            }
        }
        None
    }

    fn app_mut(&mut self, name: &str) -> Option<&mut Application> {
        for (n, a) in &mut self.applications {
            if n == name {
                return Some(a);
            }
        }
        None
    }

    fn app_by_id(&self, id: &str) -> Option<&Application> {
        for (_, a) in &self.applications {
            if a.id == id {
                return Some(a);
            }
        }
        self.detached_apps.iter().find(|a| a.id == id)
    }

    fn groups_for(&self, app_name: &str) -> Option<&Vec<(String, DeploymentGroup)>> {
        for (n, groups) in &self.deployment_groups {
            if n == app_name {
                return Some(groups);
            }
        }
        None
    }

    fn groups_for_mut(&mut self, app_name: &str) -> Option<&mut Vec<(String, DeploymentGroup)>> {
        for (n, groups) in &mut self.deployment_groups {
            if n == app_name {
                return Some(groups);
            }
        }
        None
    }

    fn deployment(&self, id: &str) -> Option<&DeploymentInfo> {
        for (i, d) in &self.deployments {
            if i == id {
                return Some(d);
            }
        }
        None
    }

    fn deployment_mut(&mut self, id: &str) -> Option<&mut DeploymentInfo> {
        for (i, d) in &mut self.deployments {
            if i == id {
                return Some(d);
            }
        }
        None
    }

    fn group_by_id(&self, id: &str) -> Option<&DeploymentGroup> {
        for (_, groups) in &self.deployment_groups {
            for (_, g) in groups {
                if g.deployment_group_id == id {
                    return Some(g);
                }
            }
        }
        self.detached_groups
            .iter()
            .find(|g| g.deployment_group_id == id)
    }

    fn config(&self, name: &str) -> Option<&DeploymentConfig> {
        for (n, c) in &self.deployment_configs {
            if n == name {
                return Some(c);
            }
        }
        None
    }

    fn on_prem(&self, name: &str) -> Option<&OnPremisesInstance> {
        for (n, i) in &self.on_premises {
            if n == name {
                return Some(i);
            }
        }
        None
    }

    fn on_prem_mut(&mut self, name: &str) -> Option<&mut OnPremisesInstance> {
        for (n, i) in &mut self.on_premises {
            if n == name {
                return Some(i);
            }
        }
        None
    }

    // --- moto's TaggingService ----------------------------------------------

    fn tag(&mut self, arn: &str, tags: &Value) {
        let Some(list) = tags.as_array() else {
            return;
        };
        if self.tags.iter().all(|(a, _)| a != arn) {
            self.tags.push((arn.to_string(), Map::new()));
        }
        let (_, map) = self
            .tags
            .iter_mut()
            .find(|(a, _)| a == arn)
            .expect("arn just pushed");
        for tag in list {
            if let Some(key) = tag.get("Key").and_then(Value::as_str) {
                let value = tag.get("Value").cloned().unwrap_or(Value::Null);
                map.insert(key.to_string(), value);
            }
        }
    }

    fn untag(&mut self, arn: &str, keys: &[String]) {
        if let Some((_, map)) = self.tags.iter_mut().find(|(a, _)| a == arn) {
            for key in keys {
                map.remove(key);
            }
        }
    }

    fn tag_list(&self, arn: &str) -> Vec<Value> {
        self.tags
            .iter()
            .find(|(a, _)| a == arn)
            .map(|(_, m)| {
                m.iter()
                    .map(|(k, v)| json!({"Key": k, "Value": v}))
                    .collect()
            })
            .unwrap_or_default()
    }

    // --- checks mirroring moto's get_application/get_deployment_group etc. ----

    fn require_app(&self, name: &str) -> Option<Response> {
        if self.app(name).is_none() {
            return Some(app_does_not_exist(name));
        }
        None
    }

    fn require_group(&self, app_name: &str, group_name: &str) -> Option<Response> {
        if self.app(app_name).is_none() {
            return Some(app_does_not_exist(app_name));
        }
        let exists = self
            .groups_for(app_name)
            .is_some_and(|gs| gs.iter().any(|(n, _)| n == group_name));
        if !exists {
            return Some(deployment_group_does_not_exist(group_name));
        }
        None
    }

    fn require_deployment(&self, id: &str) -> Option<Response> {
        if self.deployment(id).is_none() {
            return Some(deployment_does_not_exist(id));
        }
        None
    }

    // --- per-record serializers (moto's to_dict methods) ---------------------

    fn app_value(app: &Application) -> Value {
        json!({
            "applicationId": app.id,
            "applicationName": app.name,
            "createTime": app.create_time,
            "computePlatform": app.compute_platform,
        })
    }

    fn group_value(&self, group: &DeploymentGroup) -> Value {
        let app_name = self
            .app_by_id(&group.app_id)
            .map(|a| a.name.clone())
            .unwrap_or_default();
        let compute_platform = self
            .app_by_id(&group.app_id)
            .map(|a| a.compute_platform.clone())
            .unwrap_or_default();
        json!({
            "applicationName": app_name,
            "deploymentGroupId": group.deployment_group_id,
            "deploymentGroupName": group.name,
            "deploymentConfigName": py_str(&group.deployment_config_name),
            "ec2TagFilters": group.ec2_tag_filters,
            "onPremisesInstanceTagFilters": group.on_premises_instance_tag_filters,
            "autoScalingGroups": group.auto_scaling_groups,
            "serviceRoleArn": group.service_role_arn,
            "targetRevision": {},
            "triggerConfigurations": group.trigger_configurations,
            "alarmConfiguration": {},
            "autoRollbackConfiguration": group.auto_rollback_configuration,
            "deploymentStyle": group.deployment_style,
            "outdatedInstancesStrategy": group.outdated_instances_strategy,
            "blueGreenDeploymentConfiguration": group.blue_green_deployment_configuration,
            "loadBalancerInfo": group.load_balancer_info,
            "lastSuccessfulDeployment": {},
            "lastAttemptedDeployment": {},
            "ec2TagSet": group.ec2_tag_set,
            "onPremisesTagSet": group.on_premises_tag_set,
            "computePlatform": compute_platform,
            "ecsServices": group.ecs_services,
            "terminationHookEnabled": group.termination_hook_enabled,
        })
    }

    fn deployment_value(&self, dep: &DeploymentInfo) -> Value {
        let (deployment_style, blue_green, load_balancer) = self
            .group_by_id(&dep.group_id)
            .map(|g| {
                (
                    g.deployment_style.clone(),
                    g.blue_green_deployment_configuration.clone(),
                    g.load_balancer_info.clone(),
                )
            })
            .unwrap_or((json!({}), json!({}), json!({})));
        let compute_platform = self
            .app_by_id(&dep.application_id)
            .map(|a| a.compute_platform.clone())
            .unwrap_or_default();
        json!({
            "applicationName": dep.application_name,
            "deploymentGroupName": dep.deployment_group_name,
            "deploymentConfigName": py_str(&dep.deployment_config_name),
            "deploymentId": dep.deployment_id,
            "previousRevision": {},
            "revision": dep.revision,
            "status": dep.status,
            "errorInformation": {},
            "createTime": dep.create_time,
            "startTime": Value::Null,
            "completeTime": Value::Null,
            "deploymentOverview": {
                "Pending": 0,
                "InProgress": 0,
                "Succeeded": 0,
                "Failed": 0,
                "Skipped": 0,
                "Ready": 0,
            },
            "description": dep.description,
            "creator": "user",
            "ignoreApplicationStopFailures": dep.ignore_application_stop_failures,
            "autoRollbackConfiguration": dep.auto_rollback_configuration,
            "updateOutdatedInstancesOnly": dep.update_outdated_instances_only,
            "rollbackInfo": {},
            "deploymentStyle": deployment_style,
            "targetInstances": dep.target_instances,
            "instanceTerminationWaitTimeStarted": false,
            "blueGreenDeploymentConfiguration": blue_green,
            "loadBalancerInfo": load_balancer,
            "additionalDeploymentStatusInfo": "",
            "fileExistsBehavior": dep.file_exists_behavior,
            "deploymentStatusMessages": [],
            "computePlatform": compute_platform,
            "externalId": "",
            "relatedDeployments": {},
            "overrideAlarmConfiguration": dep.override_alarm_configuration,
        })
    }

    fn instance_value(inst: &OnPremisesInstance) -> Value {
        let mut out = json!({
            "instanceName": inst.instance_name,
            "iamSessionArn": inst.iam_session_arn,
            "iamUserArn": inst.iam_user_arn,
            "registerTime": inst.register_time,
            "tags": inst.tags,
        });
        if let Some(dt) = &inst.deregister_time {
            out["deregisterTime"] = json!(dt);
        }
        out
    }

    fn config_value(config: &DeploymentConfig) -> Value {
        json!({
            "deploymentConfigId": config.deployment_config_id,
            "deploymentConfigName": config.deployment_config_name,
            "minimumHealthyHosts": config.minimum_healthy_hosts,
            "trafficRoutingConfig": config.traffic_routing_config,
            "computePlatform": config.compute_platform,
            "zonalConfig": config.zonal_config,
            "createTime": config.create_time,
        })
    }

    /// moto's default-config stub shape (fresh id per call).
    fn default_config_value(name: &str) -> Value {
        json!({
            "deploymentConfigId": uuid::Uuid::new_v4().to_string(),
            "deploymentConfigName": name,
            "minimumHealthyHosts": {"type": "HOST_COUNT", "value": 0},
            "createTime": now_iso(),
            "computePlatform": "Server",
        })
    }

    /// The Value-valued group fields moto's `update_deployment_group`
    /// replaces when present on the wire (absent/None keeps the old value).
    fn apply_group_updates(&mut self, app_name: &str, name: &str, p: &Value) {
        {
            let Some(groups) = self.groups_for_mut(app_name) else {
                return;
            };
            let Some((_, group)) = groups.iter_mut().find(|(n, _)| n == name) else {
                return;
            };
            if let Some(Value::String(s)) = p.get("deploymentConfigName") {
                group.deployment_config_name = Some(s.clone());
            }
            if let Some(Value::String(s)) = p.get("outdatedInstancesStrategy") {
                group.outdated_instances_strategy = Some(s.clone());
            }
            if let Some(Value::String(s)) = p.get("serviceRoleArn") {
                group.service_role_arn = s.clone();
            }
        }
        for (key, dest) in [
            ("ec2TagFilters", GroupField::Ec2TagFilters),
            (
                "onPremisesInstanceTagFilters",
                GroupField::OnPremisesInstanceTagFilters,
            ),
            ("autoScalingGroups", GroupField::AutoScalingGroups),
            ("triggerConfigurations", GroupField::TriggerConfigurations),
            (
                "autoRollbackConfiguration",
                GroupField::AutoRollbackConfiguration,
            ),
            ("deploymentStyle", GroupField::DeploymentStyle),
            (
                "blueGreenDeploymentConfiguration",
                GroupField::BlueGreenDeploymentConfiguration,
            ),
            ("loadBalancerInfo", GroupField::LoadBalancerInfo),
            ("ec2TagSet", GroupField::Ec2TagSet),
            ("onPremisesTagSet", GroupField::OnPremisesTagSet),
            ("ecsServices", GroupField::EcsServices),
        ] {
            let Some(v) = p.get(key) else {
                continue;
            };
            if v.is_null() {
                continue;
            }
            if let Some(groups) = self.groups_for_mut(app_name) {
                if let Some((_, group)) = groups.iter_mut().find(|(n, _)| n == name) {
                    group.set_field(dest, v.clone());
                }
            }
        }
        if let Some(v) = p.get("terminationHookEnabled") {
            if let Some(groups) = self.groups_for_mut(app_name) {
                if let Some((_, group)) = groups.iter_mut().find(|(n, _)| n == name) {
                    group.termination_hook_enabled = v.clone();
                }
            }
        }
    }
}

enum GroupField {
    Ec2TagFilters,
    OnPremisesInstanceTagFilters,
    AutoScalingGroups,
    TriggerConfigurations,
    AutoRollbackConfiguration,
    DeploymentStyle,
    BlueGreenDeploymentConfiguration,
    LoadBalancerInfo,
    Ec2TagSet,
    OnPremisesTagSet,
    EcsServices,
}

impl DeploymentGroup {
    fn set_field(&mut self, field: GroupField, value: Value) {
        match field {
            GroupField::Ec2TagFilters => self.ec2_tag_filters = value,
            GroupField::OnPremisesInstanceTagFilters => {
                self.on_premises_instance_tag_filters = value
            }
            GroupField::AutoScalingGroups => self.auto_scaling_groups = value,
            GroupField::TriggerConfigurations => self.trigger_configurations = value,
            GroupField::AutoRollbackConfiguration => self.auto_rollback_configuration = value,
            GroupField::DeploymentStyle => self.deployment_style = value,
            GroupField::BlueGreenDeploymentConfiguration => {
                self.blue_green_deployment_configuration = value
            }
            GroupField::LoadBalancerInfo => self.load_balancer_info = value,
            GroupField::Ec2TagSet => self.ec2_tag_set = value,
            GroupField::OnPremisesTagSet => self.on_premises_tag_set = value,
            GroupField::EcsServices => self.ecs_services = value,
        }
    }
}

// --- Request handling ---------------------------------------------------------

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "codedeploy" {
        return None;
    }
    let params = parse_json_body(&req.body);
    let slot = state();
    let mut guard = slot.write();
    let st = guard.entry((req.account, req.region.clone())).or_default();
    Some(handle(req, st, &params))
}

fn handle(req: &Request, st: &mut State, p: &Value) -> Response {
    match req.operation.as_str() {
        "AddTagsToOnPremisesInstances" => st.add_tags_to_on_premises(p),
        "BatchGetApplicationRevisions" => st.batch_get_application_revisions(p),
        "BatchGetApplications" => st.batch_get_applications(p),
        "BatchGetDeploymentGroups" => st.batch_get_deployment_groups(p),
        "BatchGetDeploymentInstances" => st.batch_get_deployment_instances(p),
        "BatchGetDeploymentTargets" => st.batch_get_deployment_targets(p),
        "BatchGetDeployments" => st.batch_get_deployments(p),
        "BatchGetOnPremisesInstances" => st.batch_get_on_premises_instances(p),
        "ContinueDeployment" => st.continue_deployment(p),
        "CreateApplication" => st.create_application(req, p),
        "CreateDeployment" => st.create_deployment(req, p),
        "CreateDeploymentConfig" => st.create_deployment_config(p),
        "CreateDeploymentGroup" => st.create_deployment_group(req, p),
        "DeleteApplication" => st.delete_application(p),
        "DeleteDeploymentConfig" => st.delete_deployment_config(p),
        "DeleteDeploymentGroup" => st.delete_deployment_group(p),
        "DeleteGitHubAccountToken" => st.delete_git_hub_account_token(p),
        "DeleteResourcesByExternalId" => ok(&json!({})),
        "DeregisterOnPremisesInstance" => st.deregister_on_premises_instance(p),
        "GetApplication" => st.get_application(p),
        "GetApplicationRevision" => st.get_application_revision(p),
        "GetDeployment" => st.get_deployment(p),
        "GetDeploymentConfig" => st.get_deployment_config(p),
        "GetDeploymentGroup" => st.get_deployment_group(p),
        "GetDeploymentInstance" => st.get_deployment_instance(p),
        "GetDeploymentTarget" => st.get_deployment_target(p),
        "GetOnPremisesInstance" => st.get_on_premises_instance(p),
        "ListApplicationRevisions" => st.list_application_revisions(p),
        "ListApplications" => st.list_applications(),
        "ListDeploymentConfigs" => st.list_deployment_configs(),
        "ListDeploymentGroups" => st.list_deployment_groups(p),
        "ListDeploymentInstances" => st.list_deployment_instances(p),
        "ListDeploymentTargets" => st.list_deployment_targets(p),
        "ListDeployments" => st.list_deployments(p),
        "ListGitHubAccountTokenNames" => ok(&json!({"tokenNameList": [], "nextToken": ""})),
        "ListOnPremisesInstances" => st.list_on_premises_instances(),
        "ListTagsForResource" => st.list_tags_for_resource(p),
        "PutLifecycleEventHookExecutionStatus" => st.put_lifecycle_event_hook_execution_status(p),
        "RegisterApplicationRevision" => st.register_application_revision(p),
        "RegisterOnPremisesInstance" => st.register_on_premises_instance(p),
        "RemoveTagsFromOnPremisesInstances" => st.remove_tags_from_on_premises_instances(p),
        "SkipWaitTimeForInstanceTermination" => st.skip_wait_time_for_instance_termination(p),
        "StopDeployment" => st.stop_deployment(p),
        "TagResource" => st.tag_resource(p),
        "UntagResource" => st.untag_resource(p),
        "UpdateApplication" => st.update_application(p),
        "UpdateDeploymentGroup" => st.update_deployment_group(p),
        _ => json_error(
            501,
            "NotImplemented",
            &format!("The {} action has not been implemented", req.operation),
        ),
    }
}

impl State {
    fn app_arn(region: &str, account: &str, name: &str) -> String {
        format!("arn:aws:codedeploy:{region}:{account}:application:{name}")
    }

    fn dg_arn(region: &str, account: &str, app: &str, dg: &str) -> String {
        format!("arn:aws:codedeploy:{region}:{account}:deploymentgroup:{app}/{dg}")
    }

    fn deployment_arn(region: &str, account: &str, id: &str) -> String {
        format!("arn:aws:codedeploy:{region}:{account}:deployment:{id}")
    }

    // --- Applications -------------------------------------------------------

    fn create_application(&mut self, req: &Request, p: &Value) -> Response {
        let name = opt_str(p, "applicationName").unwrap_or_default();
        if self.app(&name).is_some() {
            return app_already_exists(&name);
        }
        let id = uuid::Uuid::new_v4().to_string();
        let app = Application {
            id: id.clone(),
            name: name.clone(),
            compute_platform: p.get("computePlatform").cloned().unwrap_or(Value::Null),
            create_time: now_iso(),
            revisions: HashMap::new(),
        };
        self.applications.push((name.clone(), app));
        if let Some(tags) = p.get("tags") {
            if truthy(tags) {
                let arn = Self::app_arn(&req.region, &req.account.to_string(), &name);
                self.tag(&arn, tags);
            }
        }
        ok(&json!({"applicationId": id}))
    }

    fn get_application(&mut self, p: &Value) -> Response {
        let name = opt_str(p, "applicationName").unwrap_or_default();
        let Some(app) = self.app(&name) else {
            return app_does_not_exist(&name);
        };
        let value = Self::app_value(app);
        ok(&json!({"application": value}))
    }

    fn batch_get_applications(&mut self, p: &Value) -> Response {
        let names = p.get("applicationNames").and_then(Value::as_array);
        let mut info = Vec::new();
        for name in names.into_iter().flatten() {
            let name = name.as_str().unwrap_or_default();
            let Some(app) = self.app(name) else {
                return app_does_not_exist(name);
            };
            info.push(Self::app_value(app));
        }
        ok(&json!({"applicationsInfo": info}))
    }

    fn list_applications(&self) -> Response {
        let names: Vec<Value> = self.applications.iter().map(|(n, _)| json!(n)).collect();
        ok(&json!({"applications": names}))
    }

    fn update_application(&mut self, p: &Value) -> Response {
        let old = opt_str(p, "applicationName").unwrap_or_default();
        if old.is_empty() {
            return ok(&json!({})); // moto: an empty name is a silent no-op
        }
        if self.app(&old).is_none() {
            return app_does_not_exist(&old);
        }
        if let Some(new) = opt_str(p, "newApplicationName") {
            if new != old {
                if self.app(&new).is_some() {
                    return app_already_exists(&new);
                }
                // pop + rename + append (moto's dict re-assignment appends)
                let pos = self
                    .applications
                    .iter()
                    .position(|(n, _)| *n == old)
                    .expect("app checked above");
                let (_, mut app) = self.applications.remove(pos);
                app.name = new.clone();
                self.applications.push((new.clone(), app));
                // re-key the group row for this app
                if let Some(pos) = self.deployment_groups.iter().position(|(n, _)| *n == old) {
                    let item = self.deployment_groups.remove(pos);
                    self.deployment_groups.push((new, item.1));
                }
            }
        }
        ok(&json!({}))
    }

    fn delete_application(&mut self, p: &Value) -> Response {
        let name = opt_str(p, "applicationName").unwrap_or_default();
        let Some(pos) = self.applications.iter().position(|(n, _)| *n == name) else {
            return ok(&json!({})); // AWS does not raise for a missing app
        };
        let (_, app) = self.applications.remove(pos);
        self.detached_apps.push(app);
        if let Some(pos) = self.deployment_groups.iter().position(|(n, _)| *n == name) {
            let item = self.deployment_groups.remove(pos);
            self.detached_groups
                .extend(item.1.into_iter().map(|(_, g)| g));
        }
        // deployments bake their application name at creation (moto quirk)
        self.deployments.retain(|(_, d)| d.application_name != name);
        ok(&json!({}))
    }

    // --- Deployment groups --------------------------------------------------

    fn create_deployment_group(&mut self, req: &Request, p: &Value) -> Response {
        let app_name = opt_str(p, "applicationName").unwrap_or_default();
        if let Some(resp) = self.require_app(&app_name) {
            return resp;
        }
        let group_name = opt_str(p, "deploymentGroupName").unwrap_or_default();
        if self
            .groups_for(&app_name)
            .is_some_and(|gs| gs.iter().any(|(n, _)| *n == group_name))
        {
            return deployment_group_already_exists(&group_name);
        }
        let config_name = opt_str(p, "deploymentConfigName")
            .filter(|name| !name.is_empty())
            .unwrap_or_else(|| "CodeDeployDefault.OneAtATime".to_string());
        let id = uuid::Uuid::new_v4().to_string();
        let group = DeploymentGroup {
            app_id: self
                .app(&app_name)
                .map(|a| a.id.clone())
                .unwrap_or_default(),
            name: group_name.clone(),
            deployment_group_id: id.clone(),
            deployment_config_name: Some(config_name),
            ec2_tag_filters: param_or(p.get("ec2TagFilters"), json!([])),
            on_premises_instance_tag_filters: param_or(
                p.get("onPremisesInstanceTagFilters"),
                json!([]),
            ),
            auto_scaling_groups: param_or(p.get("autoScalingGroups"), json!([])),
            service_role_arn: opt_str(p, "serviceRoleArn").unwrap_or_default(),
            trigger_configurations: param_or(p.get("triggerConfigurations"), json!([])),
            auto_rollback_configuration: param_or(p.get("autoRollbackConfiguration"), json!({})),
            outdated_instances_strategy: opt_str(p, "outdatedInstancesStrategy"),
            deployment_style: param_or(p.get("deploymentStyle"), json!({})),
            blue_green_deployment_configuration: param_or(
                p.get("blueGreenDeploymentConfiguration"),
                json!({}),
            ),
            load_balancer_info: param_or(p.get("loadBalancerInfo"), json!({})),
            ec2_tag_set: param_or(p.get("ec2TagSet"), json!({})),
            ecs_services: param_or(p.get("ecsServices"), json!([])),
            on_premises_tag_set: param_or(p.get("onPremisesTagSet"), json!({})),
            tags: param_or(p.get("tags"), json!([])),
            termination_hook_enabled: p
                .get("terminationHookEnabled")
                .cloned()
                .unwrap_or(Value::Null),
        };
        if let Some(groups) = self.groups_for_mut(&app_name) {
            groups.push((group_name.clone(), group));
        } else {
            self.deployment_groups
                .push((app_name.clone(), vec![(group_name.clone(), group)]));
        }
        if let Some(tags) = p.get("tags") {
            if truthy(tags) {
                let arn = Self::dg_arn(
                    &req.region,
                    &req.account.to_string(),
                    &app_name,
                    &group_name,
                );
                self.tag(&arn, tags);
            }
        }
        ok(&json!({"deploymentGroupId": id}))
    }

    fn get_deployment_group(&self, p: &Value) -> Response {
        let app_name = opt_str(p, "applicationName").unwrap_or_default();
        let group_name = opt_str(p, "deploymentGroupName").unwrap_or_default();
        if let Some(resp) = self.require_group(&app_name, &group_name) {
            return resp;
        }
        let value = self
            .groups_for(&app_name)
            .and_then(|gs| gs.iter().find(|(n, _)| *n == group_name))
            .map(|(_, g)| self.group_value(g))
            .unwrap_or(json!({}));
        ok(&json!({"deploymentGroupInfo": value}))
    }

    fn delete_deployment_group(&mut self, p: &Value) -> Response {
        let app_name = opt_str(p, "applicationName").unwrap_or_default();
        let group_name = opt_str(p, "deploymentGroupName").unwrap_or_default();
        if let Some(resp) = self.require_app(&app_name) {
            return resp;
        }
        let mut detached: Option<DeploymentGroup> = None;
        if let Some(groups) = self.groups_for_mut(&app_name) {
            if let Some(pos) = groups.iter().position(|(n, _)| *n == group_name) {
                detached = groups.remove(pos).1.into();
            }
        }
        if let Some(group) = detached {
            self.detached_groups.push(group);
        }
        ok(&json!({"hooksNotCleanedUp": []}))
    }

    fn update_deployment_group(&mut self, p: &Value) -> Response {
        let app_name = opt_str(p, "applicationName").unwrap_or_default();
        let current = opt_str(p, "currentDeploymentGroupName").unwrap_or_default();
        if let Some(resp) = self.require_group(&app_name, &current) {
            return resp;
        }
        let mut effective = current.clone();
        if let Some(new_name) = opt_str(p, "newDeploymentGroupName").filter(|n| !n.is_empty()) {
            if new_name != current {
                if self
                    .groups_for(&app_name)
                    .is_some_and(|gs| gs.iter().any(|(n, _)| *n == new_name))
                {
                    return deployment_group_already_exists(&new_name);
                }
                if let Some(groups) = self.groups_for_mut(&app_name) {
                    if let Some(pos) = groups.iter().position(|(n, _)| *n == current) {
                        let mut group = groups.remove(pos).1;
                        group.name = new_name.clone();
                        effective = new_name.clone();
                        groups.push((new_name, group));
                    }
                }
            }
        }
        self.apply_group_updates(&app_name, &effective, p);
        ok(&json!({"hooksNotCleanedUp": []}))
    }

    fn list_deployment_groups(&self, p: &Value) -> Response {
        let app_name = opt_str(p, "applicationName").unwrap_or_default();
        let next_token = p.get("nextToken").cloned().unwrap_or(json!(""));
        let names: Vec<Value> = self
            .groups_for(&app_name)
            .map(|gs| gs.iter().map(|(n, _)| json!(n)).collect())
            .unwrap_or_default();
        ok(&json!({
            "applicationName": app_name,
            "deploymentGroups": names,
            "nextToken": next_token,
        }))
    }

    fn batch_get_deployment_groups(&self, p: &Value) -> Response {
        let app_name = opt_str(p, "applicationName").unwrap_or_default();
        if let Some(resp) = self.require_app(&app_name) {
            return resp;
        }
        let names = p.get("deploymentGroupNames").and_then(Value::as_array);
        let mut info = Vec::new();
        if let Some(groups) = self.groups_for(&app_name) {
            for name in names.into_iter().flatten() {
                let key = name.as_str().unwrap_or_default();
                if let Some((_, g)) = groups.iter().find(|(n, _)| n == key) {
                    info.push(self.group_value(g));
                }
            }
        }
        ok(&json!({"deploymentGroupsInfo": info, "errorMessage": ""}))
    }

    // --- Deployments --------------------------------------------------------

    fn create_deployment(&mut self, req: &Request, p: &Value) -> Response {
        let app_name = opt_str(p, "applicationName").unwrap_or_default();
        if let Some(resp) = self.require_app(&app_name) {
            return resp;
        }
        let group_name = opt_str(p, "deploymentGroupName").unwrap_or_default();
        if group_name.is_empty() {
            return deployment_group_name_required("Deployment group name is required.");
        }
        if !self
            .groups_for(&app_name)
            .is_some_and(|gs| gs.iter().any(|(n, _)| *n == group_name))
        {
            // moto does not interpolate the group name into this message
            return deployment_group_does_not_exist("Deployment group name does not exist.");
        }
        let deployment_config_name =
            opt_str(p, "deploymentConfigName").filter(|name| !name.is_empty());
        let deployment_config_name = match deployment_config_name {
            Some(config) => Some(config),
            None => self
                .groups_for(&app_name)
                .and_then(|gs| gs.iter().find(|(n, _)| *n == group_name))
                .and_then(|(_, g)| g.deployment_config_name.clone()),
        };
        let id = uuid::Uuid::new_v4().to_string();
        let dep = DeploymentInfo {
            application_name: app_name.clone(),
            deployment_group_name: group_name.clone(),
            deployment_config_name,
            deployment_id: id.clone(),
            application_id: self
                .app(&app_name)
                .map(|a| a.id.clone())
                .unwrap_or_default(),
            group_id: self
                .groups_for(&app_name)
                .and_then(|gs| gs.iter().find(|(n, _)| *n == group_name))
                .map(|(_, g)| g.deployment_group_id.clone())
                .unwrap_or_default(),
            revision: p.get("revision").cloned(),
            status: "Created".to_string(),
            create_time: now_iso(),
            description: opt_str(p, "description"),
            ignore_application_stop_failures: p.get("ignoreApplicationStopFailures").cloned(),
            target_instances: p.get("targetInstances").cloned(),
            auto_rollback_configuration: p.get("autoRollbackConfiguration").cloned(),
            update_outdated_instances_only: p.get("updateOutdatedInstancesOnly").cloned(),
            file_exists_behavior: opt_str(p, "fileExistsBehavior"),
            override_alarm_configuration: p.get("overrideAlarmConfiguration").cloned(),
        };
        self.deployments.push((id.clone(), dep));
        // moto copies the group's tags onto the deployment's ARN
        let group_tags = self
            .groups_for(&app_name)
            .and_then(|gs| gs.iter().find(|(n, _)| *n == group_name))
            .map(|(_, g)| g.tags.clone());
        if let Some(tags) = group_tags {
            if truthy(&tags) {
                let arn = Self::deployment_arn(&req.region, &req.account.to_string(), &id);
                self.tag(&arn, &tags);
            }
        }
        ok(&json!({"deploymentId": id}))
    }

    fn get_deployment(&self, p: &Value) -> Response {
        let id = opt_str(p, "deploymentId").unwrap_or_default();
        let Some(dep) = self.deployment(&id) else {
            return deployment_does_not_exist(&id);
        };
        ok(&json!({"deploymentInfo": self.deployment_value(dep)}))
    }

    fn batch_get_deployments(&self, p: &Value) -> Response {
        let ids = p.get("deploymentIds").and_then(Value::as_array);
        let mut info = Vec::new();
        for id in ids.into_iter().flatten() {
            let id = id.as_str().unwrap_or_default();
            if let Some(dep) = self.deployment(id) {
                info.push(self.deployment_value(dep));
            }
        }
        ok(&json!({"deploymentsInfo": info}))
    }

    fn list_deployments(&self, p: &Value) -> Response {
        let app_name = opt_str(p, "applicationName").unwrap_or_default();
        let group_name = opt_str(p, "deploymentGroupName").unwrap_or_default();
        if !app_name.is_empty() && group_name.is_empty() {
            return deployment_group_name_required(
                "If applicationName is specified, then deploymentGroupName must be specified.",
            );
        }
        if !group_name.is_empty() && app_name.is_empty() {
            return app_name_required();
        }
        let statuses: Vec<String> = p
            .get("includeOnlyStatuses")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(Value::as_str)
                    .map(String::from)
                    .collect()
            })
            .unwrap_or_default();
        let mut ids = Vec::new();
        for (_, dep) in &self.deployments {
            if !app_name.is_empty() && dep.application_name != app_name {
                continue;
            }
            if !group_name.is_empty() {
                // moto scopes the remaining checks to this block
                let Some(groups) = self.groups_for(&app_name) else {
                    continue;
                };
                if !groups.iter().any(|(n, _)| *n == group_name) {
                    continue;
                }
                if dep.deployment_group_name != group_name {
                    continue;
                }
                if !statuses.is_empty() && !statuses.contains(&dep.status) {
                    continue;
                }
            }
            ids.push(json!(dep.deployment_id));
        }
        ok(&json!({"deployments": ids}))
    }

    fn stop_deployment(&mut self, p: &Value) -> Response {
        let id = opt_str(p, "deploymentId").unwrap_or_default();
        if let Some(resp) = self.require_deployment(&id) {
            return resp;
        }
        if let Some(dep) = self.deployment_mut(&id) {
            dep.status = "Stopped".to_string();
        }
        ok(&json!({
            "status": "Succeeded",
            "statusMessage": "The deployment was stopped successfully.",
        }))
    }

    fn continue_deployment(&self, p: &Value) -> Response {
        if let Some(id) = opt_str(p, "deploymentId") {
            if !id.is_empty() && self.deployment(&id).is_none() {
                return deployment_does_not_exist(&id);
            }
        }
        ok(&json!({}))
    }

    fn skip_wait_time_for_instance_termination(&self, p: &Value) -> Response {
        if let Some(id) = opt_str(p, "deploymentId") {
            if !id.is_empty() && self.deployment(&id).is_none() {
                return deployment_does_not_exist(&id);
            }
        }
        ok(&json!({}))
    }

    fn put_lifecycle_event_hook_execution_status(&self, p: &Value) -> Response {
        if let Some(id) = opt_str(p, "deploymentId") {
            if !id.is_empty() && self.deployment(&id).is_none() {
                return deployment_does_not_exist(&id);
            }
        }
        let hook_id = opt_str(p, "lifecycleEventHookExecutionId")
            .filter(|id| !id.is_empty())
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        ok(&json!({"lifecycleEventHookExecutionId": hook_id}))
    }

    // instance/target stubs — instances are not tracked in detail (moto)

    fn batch_get_deployment_instances(&self, p: &Value) -> Response {
        let id = opt_str(p, "deploymentId").unwrap_or_default();
        if let Some(resp) = self.require_deployment(&id) {
            return resp;
        }
        ok(&json!({"instancesSummary": [], "errorMessage": ""}))
    }

    fn batch_get_deployment_targets(&self, p: &Value) -> Response {
        let id = opt_str(p, "deploymentId").unwrap_or_default();
        if let Some(resp) = self.require_deployment(&id) {
            return resp;
        }
        ok(&json!({"deploymentTargets": []}))
    }

    fn list_deployment_instances(&self, p: &Value) -> Response {
        let id = opt_str(p, "deploymentId").unwrap_or_default();
        if let Some(resp) = self.require_deployment(&id) {
            return resp;
        }
        ok(&json!({"instancesList": [], "nextToken": ""}))
    }

    fn list_deployment_targets(&self, p: &Value) -> Response {
        let id = opt_str(p, "deploymentId").unwrap_or_default();
        if let Some(resp) = self.require_deployment(&id) {
            return resp;
        }
        ok(&json!({"targetIds": [], "nextToken": ""}))
    }

    fn get_deployment_instance(&self, p: &Value) -> Response {
        let id = opt_str(p, "deploymentId").unwrap_or_default();
        if let Some(resp) = self.require_deployment(&id) {
            return resp;
        }
        let instance_id = p.get("instanceId").cloned().unwrap_or(Value::Null);
        ok(&json!({"instanceSummary": {
            "deploymentId": id,
            "instanceId": instance_id,
            "status": "Succeeded",
            "lastUpdatedAt": now_iso(),
            "lifecycleEvents": [],
        }}))
    }

    fn get_deployment_target(&self, p: &Value) -> Response {
        let id = opt_str(p, "deploymentId").unwrap_or_default();
        if let Some(resp) = self.require_deployment(&id) {
            return resp;
        }
        let target_id = p.get("targetId").cloned().unwrap_or(Value::Null);
        ok(&json!({"deploymentTarget": {
            "deploymentTargetType": "InstanceTarget",
            "instanceTarget": {
                "deploymentId": id,
                "targetId": target_id.clone(),
                "targetArn": target_id,
                "status": "Succeeded",
                "lastUpdatedAt": now_iso(),
                "lifecycleEvents": [],
            },
        }}))
    }

    // --- Deployment configs -------------------------------------------------

    fn create_deployment_config(&mut self, p: &Value) -> Response {
        let name = opt_str(p, "deploymentConfigName").unwrap_or_default();
        if self.config(&name).is_some() || DEFAULT_CONFIGS.contains(&name.as_str()) {
            return config_already_exists(&name);
        }
        let id = uuid::Uuid::new_v4().to_string();
        let config = DeploymentConfig {
            deployment_config_id: id.clone(),
            deployment_config_name: name.clone(),
            minimum_healthy_hosts: param_or(p.get("minimumHealthyHosts"), json!({})),
            traffic_routing_config: param_or(p.get("trafficRoutingConfig"), json!({})),
            compute_platform: param_or(p.get("computePlatform"), json!("Server")),
            zonal_config: param_or(p.get("zonalConfig"), json!({})),
            create_time: now_iso(),
        };
        self.deployment_configs.push((name, config));
        ok(&json!({"deploymentConfigId": id}))
    }

    fn get_deployment_config(&self, p: &Value) -> Response {
        let name = opt_str(p, "deploymentConfigName").unwrap_or_default();
        if DEFAULT_CONFIGS.contains(&name.as_str()) {
            return ok(&json!({"deploymentConfigInfo": Self::default_config_value(&name)}));
        }
        let Some(config) = self.config(&name) else {
            return config_does_not_exist(&name);
        };
        ok(&json!({"deploymentConfigInfo": Self::config_value(config)}))
    }

    fn delete_deployment_config(&mut self, p: &Value) -> Response {
        let name = opt_str(p, "deploymentConfigName").unwrap_or_default();
        if DEFAULT_CONFIGS.contains(&name.as_str()) {
            return config_in_use(&format!(
                "The deployment configuration {name} is a predefined configuration \
                 and cannot be deleted."
            ));
        }
        if self.config(&name).is_none() {
            return config_does_not_exist(&name);
        }
        for (_, groups) in &self.deployment_groups {
            for (_, g) in groups {
                if py_str(&g.deployment_config_name) == name {
                    return config_in_use(&format!(
                        "The deployment configuration {name} is in use."
                    ));
                }
            }
        }
        self.deployment_configs.retain(|(n, _)| n != &name);
        ok(&json!({}))
    }

    fn list_deployment_configs(&self) -> Response {
        let mut list: Vec<Value> = DEFAULT_CONFIGS.iter().map(|c| json!(c)).collect();
        list.extend(self.deployment_configs.iter().map(|(n, _)| json!(n)));
        ok(&json!({"deploymentConfigsList": list, "nextToken": ""}))
    }

    // --- On-premises instances ----------------------------------------------

    fn register_on_premises_instance(&mut self, p: &Value) -> Response {
        let name = opt_str(p, "instanceName").unwrap_or_default();
        if name.is_empty() {
            return instance_name_required();
        }
        // moto's dict assignment replaces any prior registration
        self.on_premises.retain(|(n, _)| n != &name);
        self.on_premises.push((
            name.clone(),
            OnPremisesInstance {
                instance_name: name,
                iam_session_arn: opt_str(p, "iamSessionArn").unwrap_or_default(),
                iam_user_arn: opt_str(p, "iamUserArn").unwrap_or_default(),
                register_time: now_iso(),
                deregister_time: None,
                tags: Vec::new(),
            },
        ));
        ok(&json!({}))
    }

    fn get_on_premises_instance(&self, p: &Value) -> Response {
        let name = opt_str(p, "instanceName").unwrap_or_default();
        if name.is_empty() {
            return instance_name_required();
        }
        let Some(inst) = self.on_prem(&name) else {
            return instance_not_registered(&name);
        };
        let value = Self::instance_value(inst);
        ok(&json!({"instanceInfo": value}))
    }

    fn deregister_on_premises_instance(&mut self, p: &Value) -> Response {
        let name = opt_str(p, "instanceName").unwrap_or_default();
        if name.is_empty() {
            return instance_name_required();
        }
        self.on_premises.retain(|(n, _)| n != &name);
        ok(&json!({}))
    }

    fn list_on_premises_instances(&self) -> Response {
        let names: Vec<Value> = self.on_premises.iter().map(|(n, _)| json!(n)).collect();
        ok(&json!({"instanceNames": names, "nextToken": ""}))
    }

    fn batch_get_on_premises_instances(&self, p: &Value) -> Response {
        let names = p.get("instanceNames").and_then(Value::as_array);
        let mut info = Vec::new();
        for name in names.into_iter().flatten() {
            let key = name.as_str().unwrap_or_default();
            if let Some(inst) = self.on_prem(key) {
                info.push(Self::instance_value(inst));
            }
        }
        ok(&json!({"instanceInfos": info}))
    }

    /// moto updates an existing tag in place by Key, appending new ones.
    fn add_tags_to_on_premises(&mut self, p: &Value) -> Response {
        let tags = p.get("tags").and_then(Value::as_array).cloned();
        let names = p.get("instanceNames").and_then(Value::as_array);
        for name in names.into_iter().flatten() {
            let key_name = name.as_str().unwrap_or_default().to_string();
            if self.on_prem(&key_name).is_none() {
                continue;
            }
            let Some(list) = &tags else { continue };
            let inst = self.on_prem_mut(&key_name).expect("checked above");
            for tag in list {
                let Some(key) = tag.get("Key").and_then(Value::as_str) else {
                    continue;
                };
                let value = tag
                    .get("Value")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                let existing = inst
                    .tags
                    .iter_mut()
                    .find(|t| t.get("Key").and_then(Value::as_str) == Some(key));
                match existing {
                    Some(t) => t["Value"] = json!(value),
                    None => inst.tags.push(json!({"Key": key, "Value": value})),
                }
            }
        }
        ok(&json!({}))
    }

    fn remove_tags_from_on_premises_instances(&mut self, p: &Value) -> Response {
        let tag_keys: Vec<String> = p
            .get("tags")
            .and_then(Value::as_array)
            .map(|list| {
                list.iter()
                    .filter_map(|t| t.get("Key").and_then(Value::as_str))
                    .map(String::from)
                    .collect()
            })
            .unwrap_or_default();
        let names = p.get("instanceNames").and_then(Value::as_array);
        for name in names.into_iter().flatten() {
            let key_name = name.as_str().unwrap_or_default();
            if let Some(inst) = self.on_prem_mut(key_name) {
                inst.tags
                    .retain(|t| match t.get("Key").and_then(Value::as_str) {
                        Some(k) => !tag_keys.contains(&k.to_string()),
                        None => true,
                    });
            }
        }
        ok(&json!({}))
    }

    // --- Revisions ------------------------------------------------------------

    fn register_application_revision(&mut self, p: &Value) -> Response {
        let app_name = opt_str(p, "applicationName").unwrap_or_default();
        if let Some(resp) = self.require_app(&app_name) {
            return resp;
        }
        let revision = p.get("revision").cloned().unwrap_or(Value::Null);
        let key = revision_key(&revision);
        let description = opt_str(p, "description")
            .filter(|d| !d.is_empty())
            .unwrap_or_default();
        let info = json!({
            "description": description,
            "registerTime": now_iso(),
            "firstUsedTime": Value::Null,
            "lastUsedTime": Value::Null,
        });
        if let Some(app) = self.app_mut(&app_name) {
            app.revisions.insert(key, info);
        }
        ok(&json!({}))
    }

    fn get_application_revision(&self, p: &Value) -> Response {
        let app_name = opt_str(p, "applicationName").unwrap_or_default();
        if let Some(resp) = self.require_app(&app_name) {
            return resp;
        }
        let revision = p.get("revision").cloned().unwrap_or(Value::Null);
        let key = revision_key(&revision);
        let info = self
            .app(&app_name)
            .and_then(|a| a.revisions.get(&key))
            .cloned()
            .unwrap_or(json!({}));
        ok(&json!({
            "applicationName": app_name,
            "revision": revision,
            "revisionInfo": info,
        }))
    }

    fn list_application_revisions(&self, p: &Value) -> Response {
        let app_name = opt_str(p, "applicationName").unwrap_or_default();
        if let Some(resp) = self.require_app(&app_name) {
            return resp;
        }
        let count = self.app(&app_name).map(|a| a.revisions.len()).unwrap_or(0);
        let revisions: Vec<Value> = (0..count)
            .map(|_| json!({"revisionType": "S3", "s3Location": {}}))
            .collect();
        ok(&json!({"revisions": revisions, "nextToken": ""}))
    }

    fn batch_get_application_revisions(&self, p: &Value) -> Response {
        let app_name = opt_str(p, "applicationName").unwrap_or_default();
        if let Some(resp) = self.require_app(&app_name) {
            return resp;
        }
        let app = self.app(&app_name);
        let mut result = Vec::new();
        for rev in p
            .get("revisions")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let info = app
                .and_then(|a| a.revisions.get(&revision_key(rev)))
                .cloned()
                .unwrap_or(json!({}));
            result.push(json!({"revisionLocation": rev, "genericRevisionInfo": info}));
        }
        ok(&json!({
            "applicationName": p.get("applicationName").cloned().unwrap_or(Value::Null),
            "errorMessage": "",
            "revisions": result,
        }))
    }

    // --- Tagging (PascalCase members, moto's TaggingService) -------------------

    fn tag_resource(&mut self, p: &Value) -> Response {
        let arn = opt_str(p, "ResourceArn").unwrap_or_default();
        if let Some(tags) = p.get("Tags") {
            self.tag(&arn, tags);
        }
        ok(&json!({}))
    }

    fn untag_resource(&mut self, p: &Value) -> Response {
        let arn = opt_str(p, "ResourceArn").unwrap_or_default();
        let keys: Vec<String> = p
            .get("TagKeys")
            .and_then(Value::as_array)
            .map(|list| {
                list.iter()
                    .filter_map(Value::as_str)
                    .map(String::from)
                    .collect()
            })
            .unwrap_or_default();
        self.untag(&arn, &keys);
        ok(&json!({}))
    }

    fn list_tags_for_resource(&self, p: &Value) -> Response {
        let arn = opt_str(p, "ResourceArn").unwrap_or_default();
        ok(&json!({"Tags": self.tag_list(&arn)}))
    }

    // --- Misc -------------------------------------------------------------------

    fn delete_git_hub_account_token(&self, p: &Value) -> Response {
        let token = opt_str(p, "tokenName").unwrap_or_default();
        ok(&json!({"tokenName": token}))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn uname(prefix: &str) -> String {
        format!("{prefix}-{}", uuid::Uuid::new_v4().simple())
    }

    fn json_body(resp: &Response) -> Value {
        serde_json::from_slice(&resp.body).expect("json body")
    }

    fn dispatch(op: &str, body: Value) -> Response {
        let req = Request {
            service: "codedeploy".to_string(),
            operation: op.to_string(),
            method: "POST".to_string(),
            path: "/".to_string(),
            query_string: String::new(),
            headers: HashMap::new(),
            body: serde_json::to_vec(&body).unwrap().into(),
            account: 123456789008,
            region: "us-east-1".to_string(),
        };
        crate::dispatch(&req).unwrap()
    }

    fn err_code(resp: &Response) -> String {
        json_body(resp)["__type"].as_str().unwrap_or("").to_string()
    }

    #[test]
    fn application_lifecycle_and_tags() {
        let app = uname("app");
        let created = json_body(&dispatch(
            "CreateApplication",
            json!({"applicationName": app, "computePlatform": "Server"}),
        ));
        assert_eq!(created["applicationId"].as_str().unwrap().len(), 36);

        let dup = dispatch("CreateApplication", json!({"applicationName": app}));
        assert_eq!(dup.status, 400);
        assert_eq!(err_code(&dup), "ApplicationAlreadyExistsException");

        let got = json_body(&dispatch("GetApplication", json!({"applicationName": app})));
        assert_eq!(got["application"]["applicationName"], app.as_str());
        assert_eq!(got["application"]["computePlatform"], "Server");

        let batch = json_body(&dispatch(
            "BatchGetApplications",
            json!({"applicationNames": [app]}),
        ));
        assert_eq!(
            batch["applicationsInfo"][0]["applicationName"],
            app.as_str()
        );
        let missing = dispatch(
            "BatchGetApplications",
            json!({"applicationNames": ["no-such-app"]}),
        );
        assert_eq!(err_code(&missing), "ApplicationDoesNotExistException");

        let arn = format!("arn:aws:codedeploy:us-east-1:123456789008:application:{app}");
        dispatch(
            "TagResource",
            json!({"ResourceArn": arn, "Tags": [{"Key": "env", "Value": "test"}]}),
        );
        let tags = json_body(&dispatch(
            "ListTagsForResource",
            json!({"ResourceArn": arn}),
        ));
        assert_eq!(tags["Tags"][0]["Key"], "env");
        assert_eq!(tags["Tags"][0]["Value"], "test");
        dispatch(
            "UntagResource",
            json!({"ResourceArn": arn, "TagKeys": ["env"]}),
        );
        let tags = json_body(&dispatch(
            "ListTagsForResource",
            json!({"ResourceArn": arn}),
        ));
        assert!(tags["Tags"].as_array().unwrap().is_empty());

        let got = dispatch("GetApplication", json!({"applicationName": "no-such-app"}));
        assert_eq!(err_code(&got), "ApplicationDoesNotExistException");

        dispatch("DeleteApplication", json!({"applicationName": app}));
        let list = json_body(&dispatch("ListApplications", json!({})));
        assert!(list["applications"]
            .as_array()
            .unwrap()
            .iter()
            .all(|n| n != app.as_str()));
        // deleting a missing application is a silent no-op
        assert_eq!(
            dispatch("DeleteApplication", json!({"applicationName": app})).status,
            200
        );
    }

    #[test]
    fn update_application_renames() {
        let old = uname("app");
        let new = uname("renamed");
        dispatch("CreateApplication", json!({"applicationName": old}));
        dispatch(
            "UpdateApplication",
            json!({"applicationName": old, "newApplicationName": new}),
        );
        let list = json_body(&dispatch("ListApplications", json!({})));
        let names = list["applications"].as_array().unwrap();
        assert!(names.iter().any(|n| n == new.as_str()));
        assert!(names.iter().all(|n| n != old.as_str()));

        // renaming onto an existing name fails
        let other = uname("app");
        dispatch("CreateApplication", json!({"applicationName": other}));
        let dup = dispatch(
            "UpdateApplication",
            json!({"applicationName": other, "newApplicationName": new}),
        );
        assert_eq!(err_code(&dup), "ApplicationAlreadyExistsException");

        let missing = dispatch(
            "UpdateApplication",
            json!({"applicationName": "ghost", "newApplicationName": new}),
        );
        assert_eq!(err_code(&missing), "ApplicationDoesNotExistException");
    }

    #[test]
    fn deployment_group_lifecycle() {
        let app = uname("app");
        let dg = uname("dg");
        dispatch("CreateApplication", json!({"applicationName": app}));

        let missing_app = dispatch(
            "CreateDeploymentGroup",
            json!({"applicationName": "ghost", "deploymentGroupName": dg}),
        );
        assert_eq!(err_code(&missing_app), "ApplicationDoesNotExistException");

        let created = json_body(&dispatch(
            "CreateDeploymentGroup",
            json!({"applicationName": app, "deploymentGroupName": dg}),
        ));
        assert_eq!(created["deploymentGroupId"].as_str().unwrap().len(), 36);

        let dup = dispatch(
            "CreateDeploymentGroup",
            json!({"applicationName": app, "deploymentGroupName": dg}),
        );
        assert_eq!(err_code(&dup), "DeploymentGroupAlreadyExistsException");

        let got = json_body(&dispatch(
            "GetDeploymentGroup",
            json!({"applicationName": app, "deploymentGroupName": dg}),
        ));
        let info = &got["deploymentGroupInfo"];
        assert_eq!(info["applicationName"], app.as_str());
        assert_eq!(info["deploymentGroupName"], dg.as_str());
        assert_eq!(info["deploymentConfigName"], "CodeDeployDefault.OneAtATime");
        assert_eq!(
            got["deploymentGroupInfo"]["ec2TagFilters"]
                .as_array()
                .unwrap()
                .len(),
            0
        );
        let missing = dispatch(
            "GetDeploymentGroup",
            json!({"applicationName": app, "deploymentGroupName": "ghost-dg"}),
        );
        assert_eq!(err_code(&missing), "DeploymentGroupDoesNotExistException");

        let list = json_body(&dispatch(
            "ListDeploymentGroups",
            json!({"applicationName": app}),
        ));
        assert!(list["deploymentGroups"]
            .as_array()
            .unwrap()
            .iter()
            .any(|n| n == dg.as_str()));

        let batch = json_body(&dispatch(
            "BatchGetDeploymentGroups",
            json!({"applicationName": app, "deploymentGroupNames": [dg]}),
        ));
        assert_eq!(
            batch["deploymentGroupsInfo"][0]["deploymentGroupName"],
            dg.as_str()
        );
        assert_eq!(batch["errorMessage"], "");

        dispatch(
            "DeleteDeploymentGroup",
            json!({"applicationName": app, "deploymentGroupName": dg}),
        );
        let list = json_body(&dispatch(
            "ListDeploymentGroups",
            json!({"applicationName": app}),
        ));
        assert!(list["deploymentGroups"]
            .as_array()
            .unwrap()
            .iter()
            .all(|n| n != dg.as_str()));
    }

    #[test]
    fn update_deployment_group_renames_and_updates_fields() {
        let app = uname("app");
        let dg = uname("dg");
        let dg2 = uname("dg");
        dispatch("CreateApplication", json!({"applicationName": app}));
        dispatch(
            "CreateDeploymentGroup",
            json!({"applicationName": app, "deploymentGroupName": dg}),
        );
        dispatch(
            "UpdateDeploymentGroup",
            json!({
                "applicationName": app,
                "currentDeploymentGroupName": dg,
                "newDeploymentGroupName": dg2,
                "deploymentConfigName": "CodeDeployDefault.AllAtOnce",
                "ec2TagFilters": [{"Key": "role", "Value": "web"}],
            }),
        );
        let got = json_body(&dispatch(
            "GetDeploymentGroup",
            json!({"applicationName": app, "deploymentGroupName": dg2}),
        ));
        assert_eq!(
            got["deploymentGroupInfo"]["deploymentConfigName"],
            "CodeDeployDefault.AllAtOnce"
        );
        assert_eq!(
            got["deploymentGroupInfo"]["ec2TagFilters"][0]["Value"],
            "web"
        );
        let list = json_body(&dispatch(
            "ListDeploymentGroups",
            json!({"applicationName": app}),
        ));
        assert!(list["deploymentGroups"]
            .as_array()
            .unwrap()
            .iter()
            .all(|n| n != dg.as_str()));
    }

    #[test]
    fn deployment_lifecycle_and_list_filters() {
        let app = uname("app");
        let dg = uname("dg");
        dispatch(
            "CreateApplication",
            json!({"applicationName": app, "computePlatform": "Server"}),
        );
        dispatch(
            "CreateDeploymentGroup",
            json!({"applicationName": app, "deploymentGroupName": dg}),
        );
        let created = json_body(&dispatch(
            "CreateDeployment",
            json!({
                "applicationName": app,
                "deploymentGroupName": dg,
                "revision": {"revisionType": "GitHub", "gitHubLocation": {"repository": "r", "commitId": "c"}},
            }),
        ));
        let dep_id = created["deploymentId"].as_str().unwrap().to_string();

        let got = json_body(&dispatch("GetDeployment", json!({"deploymentId": dep_id})));
        let info = &got["deploymentInfo"];
        assert_eq!(info["deploymentId"], dep_id.as_str());
        assert_eq!(info["applicationName"], app.as_str());
        assert_eq!(info["status"], "Created");
        assert_eq!(info["deploymentConfigName"], "CodeDeployDefault.OneAtATime");
        assert_eq!(info["deploymentOverview"]["Pending"], 0);
        assert_eq!(info["computePlatform"], "Server");

        let batch = json_body(&dispatch(
            "BatchGetDeployments",
            json!({"deploymentIds": [dep_id]}),
        ));
        assert_eq!(batch["deploymentsInfo"][0]["deploymentId"], dep_id.as_str());

        let listed = json_body(&dispatch(
            "ListDeployments",
            json!({"applicationName": app, "deploymentGroupName": dg, "includeOnlyStatuses": ["Created"]}),
        ));
        assert!(listed["deployments"]
            .as_array()
            .unwrap()
            .iter()
            .any(|i| i == dep_id.as_str()));
        let filtered = json_body(&dispatch(
            "ListDeployments",
            json!({"applicationName": app, "deploymentGroupName": dg, "includeOnlyStatuses": ["Succeeded"]}),
        ));
        assert!(filtered["deployments"]
            .as_array()
            .unwrap()
            .iter()
            .all(|i| i != dep_id.as_str()));

        let name_req = dispatch("ListDeployments", json!({"applicationName": app}));
        assert_eq!(err_code(&name_req), "DeploymentGroupNameRequiredException");
        let app_req = dispatch("ListDeployments", json!({"deploymentGroupName": dg}));
        assert_eq!(err_code(&app_req), "ApplicationNameRequiredException");

        let stopped = json_body(&dispatch("StopDeployment", json!({"deploymentId": dep_id})));
        assert_eq!(stopped["status"], "Succeeded");
        let got = json_body(&dispatch("GetDeployment", json!({"deploymentId": dep_id})));
        assert_eq!(got["deploymentInfo"]["status"], "Stopped");

        let missing = dispatch("GetDeployment", json!({"deploymentId": "nope"}));
        assert_eq!(err_code(&missing), "DeploymentDoesNotExistException");
    }

    #[test]
    fn deployment_group_name_is_required_for_create_deployment() {
        let app = uname("app");
        dispatch("CreateApplication", json!({"applicationName": app}));
        let no_group = dispatch(
            "CreateDeployment",
            json!({"applicationName": app, "revision": {}}),
        );
        assert_eq!(err_code(&no_group), "DeploymentGroupNameRequiredException");
        let bad_group = dispatch(
            "CreateDeployment",
            json!({"applicationName": app, "deploymentGroupName": "ghost"}),
        );
        assert_eq!(err_code(&bad_group), "DeploymentGroupDoesNotExistException");
        let bad_app = dispatch(
            "CreateDeployment",
            json!({"applicationName": "ghost", "deploymentGroupName": "x"}),
        );
        assert_eq!(err_code(&bad_app), "ApplicationDoesNotExistException");
    }

    #[test]
    fn deployment_configs_defaults_and_deletion() {
        let list = json_body(&dispatch("ListDeploymentConfigs", json!({})));
        let configs = list["deploymentConfigsList"].as_array().unwrap();
        assert!(configs.len() >= 17);
        assert!(configs.iter().any(|c| c == "CodeDeployDefault.AllAtOnce"));

        let got = json_body(&dispatch(
            "GetDeploymentConfig",
            json!({"deploymentConfigName": "CodeDeployDefault.HalfAtATime"}),
        ));
        assert_eq!(
            got["deploymentConfigInfo"]["deploymentConfigName"],
            "CodeDeployDefault.HalfAtATime"
        );
        assert_eq!(
            got["deploymentConfigInfo"]["minimumHealthyHosts"],
            json!({"type": "HOST_COUNT", "value": 0})
        );

        let predef = dispatch(
            "DeleteDeploymentConfig",
            json!({"deploymentConfigName": "CodeDeployDefault.HalfAtATime"}),
        );
        assert_eq!(err_code(&predef), "DeploymentConfigInUseException");

        let cfg = uname("cfg");
        let created = json_body(&dispatch(
            "CreateDeploymentConfig",
            json!({"deploymentConfigName": cfg, "minimumHealthyHosts": {"type": "HOST_COUNT", "value": 1}}),
        ));
        assert!(created["deploymentConfigId"].as_str().is_some());
        let dup = dispatch(
            "CreateDeploymentConfig",
            json!({"deploymentConfigName": cfg}),
        );
        assert_eq!(err_code(&dup), "DeploymentConfigAlreadyExistsException");
        let dup_default = dispatch(
            "CreateDeploymentConfig",
            json!({"deploymentConfigName": "CodeDeployDefault.AllAtOnce"}),
        );
        assert_eq!(
            err_code(&dup_default),
            "DeploymentConfigAlreadyExistsException"
        );

        let got = json_body(&dispatch(
            "GetDeploymentConfig",
            json!({"deploymentConfigName": cfg}),
        ));
        assert_eq!(
            got["deploymentConfigInfo"]["deploymentConfigName"],
            cfg.as_str()
        );
        assert_eq!(got["deploymentConfigInfo"]["computePlatform"], "Server");

        dispatch(
            "DeleteDeploymentConfig",
            json!({"deploymentConfigName": cfg}),
        );
        let missing = dispatch("GetDeploymentConfig", json!({"deploymentConfigName": cfg}));
        assert_eq!(err_code(&missing), "DeploymentConfigDoesNotExistException");
        let missing_del = dispatch(
            "DeleteDeploymentConfig",
            json!({"deploymentConfigName": "ghost-config"}),
        );
        assert_eq!(
            err_code(&missing_del),
            "DeploymentConfigDoesNotExistException"
        );
    }

    #[test]
    fn on_premises_lifecycle() {
        let name = uname("onprem");
        dispatch(
            "RegisterOnPremisesInstance",
            json!({"instanceName": name, "iamUserArn": "arn:aws:iam::123456789008:user/x"}),
        );
        let got = json_body(&dispatch(
            "GetOnPremisesInstance",
            json!({"instanceName": name}),
        ));
        assert_eq!(got["instanceInfo"]["instanceName"], name.as_str());
        assert_eq!(
            got["instanceInfo"]["iamUserArn"],
            "arn:aws:iam::123456789008:user/x"
        );

        let ghost = dispatch("GetOnPremisesInstance", json!({"instanceName": "ghost"}));
        assert_eq!(err_code(&ghost), "InstanceNotRegisteredException");

        dispatch(
            "AddTagsToOnPremisesInstances",
            json!({"instanceNames": [name], "tags": [{"Key": "env", "Value": "staging"}]}),
        );
        let got = json_body(&dispatch(
            "GetOnPremisesInstance",
            json!({"instanceName": name}),
        ));
        assert_eq!(got["instanceInfo"]["tags"][0]["Value"], "staging");

        let batch = json_body(&dispatch(
            "BatchGetOnPremisesInstances",
            json!({"instanceNames": [name]}),
        ));
        assert_eq!(batch["instanceInfos"][0]["instanceName"], name.as_str());

        dispatch(
            "RemoveTagsFromOnPremisesInstances",
            json!({"instanceNames": [name], "tags": [{"Key": "env"}]}),
        );
        let got = json_body(&dispatch(
            "GetOnPremisesInstance",
            json!({"instanceName": name}),
        ));
        assert!(got["instanceInfo"]["tags"].as_array().unwrap().is_empty());

        dispatch(
            "DeregisterOnPremisesInstance",
            json!({"instanceName": name}),
        );
        let list = json_body(&dispatch("ListOnPremisesInstances", json!({})));
        assert!(list["instanceNames"]
            .as_array()
            .unwrap()
            .iter()
            .all(|n| n != name.as_str()));
    }

    #[test]
    fn application_revisions() {
        let app = uname("app");
        dispatch("CreateApplication", json!({"applicationName": app}));
        let rev = json!({
            "revisionType": "GitHub",
            "gitHubLocation": {"repository": "r", "commitId": "c"},
        });
        let missing_app = dispatch(
            "RegisterApplicationRevision",
            json!({"applicationName": "ghost", "revision": rev}),
        );
        assert_eq!(err_code(&missing_app), "ApplicationDoesNotExistException");

        dispatch(
            "RegisterApplicationRevision",
            json!({"applicationName": app, "revision": rev}),
        );
        let got = json_body(&dispatch(
            "GetApplicationRevision",
            json!({"applicationName": app, "revision": rev}),
        ));
        assert_eq!(got["applicationName"], app.as_str());
        assert_eq!(got["revision"]["revisionType"], "GitHub");
        assert_eq!(got["revisionInfo"]["description"], "");
        assert!(got["revisionInfo"]["registerTime"].as_str().is_some());

        let list = json_body(&dispatch(
            "ListApplicationRevisions",
            json!({"applicationName": app}),
        ));
        assert_eq!(list["revisions"].as_array().unwrap().len(), 1);

        let batch = json_body(&dispatch(
            "BatchGetApplicationRevisions",
            json!({"applicationName": app, "revisions": [rev]}),
        ));
        assert_eq!(batch["applicationName"], app.as_str());
        assert_eq!(
            batch["revisions"][0]["genericRevisionInfo"]["description"],
            ""
        );
    }

    #[test]
    fn nonexistent_deployment_errors() {
        // ops that require an existing deployment (moto's exact messages)
        for (op, body) in [
            ("ContinueDeployment", json!({"deploymentId": "d-XXXXXXXXX"})),
            (
                "SkipWaitTimeForInstanceTermination",
                json!({"deploymentId": "d-XXXXXXXXX"}),
            ),
            (
                "PutLifecycleEventHookExecutionStatus",
                json!({"deploymentId": "d-XXXXXXXXX"}),
            ),
            (
                "BatchGetDeploymentInstances",
                json!({"deploymentId": "d-XXXXXXXXX"}),
            ),
            (
                "BatchGetDeploymentTargets",
                json!({"deploymentId": "d-XXXXXXXXX"}),
            ),
            (
                "ListDeploymentInstances",
                json!({"deploymentId": "d-XXXXXXXXX"}),
            ),
            (
                "ListDeploymentTargets",
                json!({"deploymentId": "d-XXXXXXXXX"}),
            ),
            (
                "GetDeploymentInstance",
                json!({"deploymentId": "d-XXXXXXXXX", "instanceId": "i-1"}),
            ),
            (
                "GetDeploymentTarget",
                json!({"deploymentId": "d-XXXXXXXXX", "targetId": "t-1"}),
            ),
            ("StopDeployment", json!({"deploymentId": "d-XXXXXXXXX"})),
        ] {
            let resp = dispatch(op, body);
            assert_eq!(resp.status, 400, "{op}");
            assert_eq!(err_code(&resp), "DeploymentDoesNotExistException", "{op}");
        }
        // absence of the deploymentId parameter is not an error (moto quirk)
        assert_eq!(dispatch("ContinueDeployment", json!({})).status, 200);
        assert_eq!(
            dispatch("SkipWaitTimeForInstanceTermination", json!({})).status,
            200
        );
        let hook = json_body(&dispatch(
            "PutLifecycleEventHookExecutionStatus",
            json!({"lifecycleEventHookExecutionId": "h-1"}),
        ));
        assert_eq!(hook["lifecycleEventHookExecutionId"], "h-1");
    }

    #[test]
    fn deployment_instance_target_stubs() {
        let app = uname("app");
        let dg = uname("dg");
        dispatch("CreateApplication", json!({"applicationName": app}));
        dispatch(
            "CreateDeploymentGroup",
            json!({"applicationName": app, "deploymentGroupName": dg}),
        );
        let dep = json_body(&dispatch(
            "CreateDeployment",
            json!({"applicationName": app, "deploymentGroupName": dg}),
        ));
        let dep_id = dep["deploymentId"].as_str().unwrap().to_string();

        let summary = json_body(&dispatch(
            "GetDeploymentInstance",
            json!({"deploymentId": dep_id, "instanceId": "i-1"}),
        ));
        assert_eq!(summary["instanceSummary"]["instanceId"], "i-1");
        assert_eq!(summary["instanceSummary"]["status"], "Succeeded");

        let target = json_body(&dispatch(
            "GetDeploymentTarget",
            json!({"deploymentId": dep_id, "targetId": "t-1"}),
        ));
        assert_eq!(
            target["deploymentTarget"]["deploymentTargetType"],
            "InstanceTarget"
        );
        assert_eq!(
            target["deploymentTarget"]["instanceTarget"]["targetId"],
            "t-1"
        );

        let instances = json_body(&dispatch(
            "ListDeploymentInstances",
            json!({"deploymentId": dep_id}),
        ));
        assert_eq!(instances["instancesList"].as_array().unwrap().len(), 0);
        assert_eq!(instances["nextToken"], "");

        let targets = json_body(&dispatch(
            "ListDeploymentTargets",
            json!({"deploymentId": dep_id}),
        ));
        assert_eq!(targets["targetIds"].as_array().unwrap().len(), 0);

        let batch_instances = json_body(&dispatch(
            "BatchGetDeploymentInstances",
            json!({"deploymentId": dep_id, "instanceIds": ["i-1"]}),
        ));
        assert_eq!(
            batch_instances["instancesSummary"]
                .as_array()
                .unwrap()
                .len(),
            0
        );

        let batch_targets = json_body(&dispatch(
            "BatchGetDeploymentTargets",
            json!({"deploymentId": dep_id, "targetIds": ["t-1"]}),
        ));
        assert_eq!(
            batch_targets["deploymentTargets"].as_array().unwrap().len(),
            0
        );

        // shared accounts tokens are unaffected by anything here
        let tokens = json_body(&dispatch("ListGitHubAccountTokenNames", json!({})));
        assert_eq!(tokens["tokenNameList"].as_array().unwrap().len(), 0);
        let deleted = json_body(&dispatch(
            "DeleteGitHubAccountToken",
            json!({"tokenName": "a-token"}),
        ));
        assert_eq!(deleted["tokenName"], "a-token");

        // unimplemented external-id cleanup answers an empty success
        let resp = dispatch(
            "DeleteResourcesByExternalId",
            json!({"externalId": "ext-1"}),
        );
        assert_eq!(resp.status, 200);
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body, json!({}));
    }
}
