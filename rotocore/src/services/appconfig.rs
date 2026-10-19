//! AWS AppConfig backend (REST-JSON, botocore requestUri + HTTP method routing).
//!
//! Port of moto's `appconfig` backend: applications, configuration profiles,
//! hosted configuration versions, environments, deployment strategies,
//! deployments, extensions, extension associations, the deprecated
//! `GetConfiguration` channel, account settings and tagging. Wire shapes
//! mirror moto's `to_json` dicts (nullable members always present); every
//! moto error renders as HTTP 400 with `__type` + `message` and an
//! `x-amzn-ErrorType` header (moto's `JsonRESTError`).
//!
//! Requests enter with an empty `operation` (the gateway keeps no REST op
//! table for appconfig), so this module resolves the operation from the
//! HTTP method + botocore requestUri — the same resolution moto's
//! `_get_action_from_method_and_request_uri` performs. Botocore
//! percent-encodes URI members (`:` → `%3A`), so tag paths stay
//! single-segment and are decoded after the split. AWS's real typo stays
//! faithful: `DeleteDeploymentStrategy` lives on
//! `/deployementstrategies/{DeploymentStrategyId}`.

use crate::store;
use crate::{Request, Response};
use once_cell::sync::Lazy;
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;

// ---------------------------------------------------------------------------
// Routing table: (method, botocore requestUri template, operation name)
// ---------------------------------------------------------------------------

static RULES: &[(&str, &str, &str)] = &[
    // Applications
    ("POST", "/applications", "CreateApplication"),
    ("GET", "/applications", "ListApplications"),
    ("GET", "/applications/{ApplicationId}", "GetApplication"),
    ("PATCH", "/applications/{ApplicationId}", "UpdateApplication"),
    ("DELETE", "/applications/{ApplicationId}", "DeleteApplication"),
    // Configuration profiles
    (
        "POST",
        "/applications/{ApplicationId}/configurationprofiles",
        "CreateConfigurationProfile",
    ),
    (
        "GET",
        "/applications/{ApplicationId}/configurationprofiles",
        "ListConfigurationProfiles",
    ),
    (
        "GET",
        "/applications/{ApplicationId}/configurationprofiles/{ConfigurationProfileId}",
        "GetConfigurationProfile",
    ),
    (
        "PATCH",
        "/applications/{ApplicationId}/configurationprofiles/{ConfigurationProfileId}",
        "UpdateConfigurationProfile",
    ),
    (
        "DELETE",
        "/applications/{ApplicationId}/configurationprofiles/{ConfigurationProfileId}",
        "DeleteConfigurationProfile",
    ),
    (
        "POST",
        "/applications/{ApplicationId}/configurationprofiles/{ConfigurationProfileId}/validators",
        "ValidateConfiguration",
    ),
    // Hosted configuration versions
    (
        "POST",
        "/applications/{ApplicationId}/configurationprofiles/{ConfigurationProfileId}/hostedconfigurationversions",
        "CreateHostedConfigurationVersion",
    ),
    (
        "GET",
        "/applications/{ApplicationId}/configurationprofiles/{ConfigurationProfileId}/hostedconfigurationversions",
        "ListHostedConfigurationVersions",
    ),
    (
        "GET",
        "/applications/{ApplicationId}/configurationprofiles/{ConfigurationProfileId}/hostedconfigurationversions/{VersionNumber}",
        "GetHostedConfigurationVersion",
    ),
    (
        "DELETE",
        "/applications/{ApplicationId}/configurationprofiles/{ConfigurationProfileId}/hostedconfigurationversions/{VersionNumber}",
        "DeleteHostedConfigurationVersion",
    ),
    // Environments
    (
        "POST",
        "/applications/{ApplicationId}/environments",
        "CreateEnvironment",
    ),
    (
        "GET",
        "/applications/{ApplicationId}/environments",
        "ListEnvironments",
    ),
    (
        "GET",
        "/applications/{ApplicationId}/environments/{EnvironmentId}",
        "GetEnvironment",
    ),
    (
        "PATCH",
        "/applications/{ApplicationId}/environments/{EnvironmentId}",
        "UpdateEnvironment",
    ),
    (
        "DELETE",
        "/applications/{ApplicationId}/environments/{EnvironmentId}",
        "DeleteEnvironment",
    ),
    // Deployments
    (
        "POST",
        "/applications/{ApplicationId}/environments/{EnvironmentId}/deployments",
        "StartDeployment",
    ),
    (
        "GET",
        "/applications/{ApplicationId}/environments/{EnvironmentId}/deployments",
        "ListDeployments",
    ),
    (
        "GET",
        "/applications/{ApplicationId}/environments/{EnvironmentId}/deployments/{DeploymentNumber}",
        "GetDeployment",
    ),
    (
        "DELETE",
        "/applications/{ApplicationId}/environments/{EnvironmentId}/deployments/{DeploymentNumber}",
        "StopDeployment",
    ),
    // Deprecated GetConfiguration channel
    (
        "GET",
        "/applications/{Application}/environments/{Environment}/configurations/{Configuration}",
        "GetConfiguration",
    ),
    // Deployment strategies
    (
        "POST",
        "/deploymentstrategies",
        "CreateDeploymentStrategy",
    ),
    (
        "GET",
        "/deploymentstrategies",
        "ListDeploymentStrategies",
    ),
    (
        "GET",
        "/deploymentstrategies/{DeploymentStrategyId}",
        "GetDeploymentStrategy",
    ),
    (
        "PATCH",
        "/deploymentstrategies/{DeploymentStrategyId}",
        "UpdateDeploymentStrategy",
    ),
    // AWS's real typo (botocore's spec): DELETE lives on deployementstrategies
    (
        "DELETE",
        "/deployementstrategies/{DeploymentStrategyId}",
        "DeleteDeploymentStrategy",
    ),
    // Extensions
    ("POST", "/extensions", "CreateExtension"),
    ("GET", "/extensions", "ListExtensions"),
    ("GET", "/extensions/{ExtensionIdentifier}", "GetExtension"),
    ("PATCH", "/extensions/{ExtensionIdentifier}", "UpdateExtension"),
    (
        "DELETE",
        "/extensions/{ExtensionIdentifier}",
        "DeleteExtension",
    ),
    // Extension associations
    (
        "POST",
        "/extensionassociations",
        "CreateExtensionAssociation",
    ),
    (
        "GET",
        "/extensionassociations",
        "ListExtensionAssociations",
    ),
    (
        "GET",
        "/extensionassociations/{ExtensionAssociationId}",
        "GetExtensionAssociation",
    ),
    (
        "PATCH",
        "/extensionassociations/{ExtensionAssociationId}",
        "UpdateExtensionAssociation",
    ),
    (
        "DELETE",
        "/extensionassociations/{ExtensionAssociationId}",
        "DeleteExtensionAssociation",
    ),
    // Account settings
    ("GET", "/settings", "GetAccountSettings"),
    ("PATCH", "/settings", "UpdateAccountSettings"),
    // Tags (ResourceArn arrives percent-encoded as a single segment)
    ("GET", "/tags/{ResourceArn}", "ListTagsForResource"),
    ("POST", "/tags/{ResourceArn}", "TagResource"),
    ("DELETE", "/tags/{ResourceArn}", "UntagResource"),
];

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

struct Application {
    id: String,
    name: String,
    description: Option<String>,
    config_profiles: BTreeMap<String, ConfigurationProfile>,
    environments: BTreeMap<String, Environment>,
}

impl Application {
    /// moto's `Application.to_json()`.
    fn describe(&self) -> Value {
        json!({
            "Id": self.id,
            "Name": self.name,
            "Description": self.description,
        })
    }
}

struct ConfigurationProfile {
    id: String,
    name: String,
    description: Option<String>,
    location_uri: Option<String>,
    retrieval_role_arn: Option<String>,
    validators: Option<Value>,
    kind: Option<String>,
    config_versions: BTreeMap<i64, HostedConfigurationVersion>,
}

impl ConfigurationProfile {
    /// moto's `ConfigurationProfile.to_json()`.
    fn describe(&self, application_id: &str) -> Value {
        json!({
            "Id": self.id,
            "Name": self.name,
            "ApplicationId": application_id,
            "Description": self.description,
            "LocationUri": self.location_uri,
            "RetrievalRoleArn": self.retrieval_role_arn,
            "Validators": self.validators,
            "Type": self.kind,
        })
    }

    /// Latest hosted version by number (moto takes the max key).
    fn latest_version(&self) -> Option<&HostedConfigurationVersion> {
        self.config_versions.values().next_back()
    }
}

struct HostedConfigurationVersion {
    app_id: String,
    config_id: String,
    version: i64,
    description: Option<String>,
    content: Vec<u8>,
    content_type: Option<String>,
    version_label: Option<String>,
}

impl HostedConfigurationVersion {
    /// moto's `HostedConfigurationVersion.to_json()` (the payload rides the
    /// raw body; metadata rides the response headers).
    fn describe(&self) -> Value {
        let mut out = json!({
            "ApplicationId": self.app_id,
            "ConfigurationProfileId": self.config_id,
            "VersionNumber": self.version,
            "ContentType": self.content_type,
        });
        if let Some(d) = self.description.clone() {
            out["Description"] = json!(d);
        }
        if let Some(l) = self.version_label.clone() {
            out["VersionLabel"] = json!(l);
        }
        out
    }

    /// moto's `HostedConfigurationVersion.get_headers()`, using botocore's
    /// case-correct header names.
    fn response_headers(&self) -> Vec<(String, String)> {
        let mut headers = vec![
            ("Application-Id".to_string(), self.app_id.clone()),
            (
                "Configuration-Profile-Id".to_string(),
                self.config_id.clone(),
            ),
            ("Version-Number".to_string(), self.version.to_string()),
        ];
        if let Some(d) = &self.description {
            headers.push(("Description".to_string(), d.clone()));
        }
        if let Some(l) = &self.version_label {
            headers.push(("VersionLabel".to_string(), l.clone()));
        }
        headers
    }
}

struct Environment {
    id: String,
    arn: String,
    application_id: String,
    name: String,
    description: Option<String>,
    monitors: Value,
    deployments: BTreeMap<i64, Deployment>,
}

impl Environment {
    /// moto's `Environment.to_json()`.
    fn describe(&self) -> Value {
        json!({
            "ApplicationId": self.application_id,
            "Id": self.id,
            "Name": self.name,
            "Description": self.description,
            "State": "READY_FOR_DEPLOYMENT",
            "Monitors": self.monitors,
        })
    }
}

struct DeploymentStrategy {
    id: String,
    name: String,
    description: Option<String>,
    deployment_duration_in_minutes: i64,
    final_bake_time_in_minutes: i64,
    growth_factor: f64,
    growth_type: String,
    replicate_to: String,
}

impl DeploymentStrategy {
    /// moto's `DeploymentStrategy.to_json()`.
    fn describe(&self) -> Value {
        json!({
            "Id": self.id,
            "Name": self.name,
            "Description": self.description,
            "DeploymentDurationInMinutes": self.deployment_duration_in_minutes,
            "GrowthType": self.growth_type,
            "GrowthFactor": self.growth_factor,
            "FinalBakeTimeInMinutes": self.final_bake_time_in_minutes,
            "ReplicateTo": self.replicate_to,
        })
    }
}

struct Deployment {
    application_id: String,
    environment_id: String,
    deployment_strategy_id: String,
    configuration_profile_id: String,
    configuration_version: Option<String>,
    description: Option<String>,
    kms_key_identifier: Option<String>,
    deployment_number: i64,
    configuration_name: String,
    configuration_location_uri: Option<String>,
    deployment_duration_in_minutes: i64,
    growth_type: String,
    growth_factor: f64,
    final_bake_time_in_minutes: i64,
    state: String,
    percentage_complete: f64,
    started_at: String,
    completed_at: String,
    version_label: Option<String>,
}

impl Deployment {
    /// moto's `Deployment.to_json()`.
    fn describe(&self) -> Value {
        json!({
            "ApplicationId": self.application_id,
            "EnvironmentId": self.environment_id,
            "DeploymentStrategyId": self.deployment_strategy_id,
            "ConfigurationProfileId": self.configuration_profile_id,
            "DeploymentNumber": self.deployment_number,
            "ConfigurationName": self.configuration_name,
            "ConfigurationLocationUri": self.configuration_location_uri,
            "ConfigurationVersion": self.configuration_version,
            "Description": self.description,
            "DeploymentDurationInMinutes": self.deployment_duration_in_minutes,
            "GrowthType": self.growth_type,
            "GrowthFactor": self.growth_factor,
            "FinalBakeTimeInMinutes": self.final_bake_time_in_minutes,
            "State": self.state,
            "EventLog": [],
            "PercentageComplete": self.percentage_complete,
            "StartedAt": self.started_at,
            "CompletedAt": self.completed_at,
            "AppliedExtensions": [],
            "KmsKeyArn": self.kms_key_identifier,
            "KmsKeyIdentifier": self.kms_key_identifier,
            "VersionLabel": self.version_label,
        })
    }

    /// moto's `Deployment.to_summary()` (no EventLog / AppliedExtensions).
    fn describe_summary(&self) -> Value {
        json!({
            "DeploymentNumber": self.deployment_number,
            "ConfigurationName": self.configuration_name,
            "ConfigurationVersion": self.configuration_version,
            "DeploymentDurationInMinutes": self.deployment_duration_in_minutes,
            "GrowthType": self.growth_type,
            "GrowthFactor": self.growth_factor,
            "FinalBakeTimeInMinutes": self.final_bake_time_in_minutes,
            "State": self.state,
            "PercentageComplete": self.percentage_complete,
            "StartedAt": self.started_at,
            "CompletedAt": self.completed_at,
            "VersionLabel": self.version_label,
        })
    }
}

struct Extension {
    id: String,
    arn: String,
    name: String,
    description: Option<String>,
    actions: Value,
    parameters: Value,
    version_number: i64,
}

impl Extension {
    /// moto's `Extension.to_json()`.
    fn describe(&self) -> Value {
        json!({
            "Id": self.id,
            "Name": self.name,
            "VersionNumber": self.version_number,
            "Arn": self.arn,
            "Description": self.description,
            "Actions": self.actions,
            "Parameters": self.parameters,
        })
    }
}

struct ExtensionAssociation {
    id: String,
    arn: String,
    extension_arn: String,
    resource_arn: String,
    parameters: Value,
    extension_version_number: i64,
}

impl ExtensionAssociation {
    /// moto's `ExtensionAssociation.to_json()`.
    fn describe(&self) -> Value {
        json!({
            "Id": self.id,
            "ExtensionArn": self.extension_arn,
            "ResourceArn": self.resource_arn,
            "Arn": self.arn,
            "Parameters": self.parameters,
            "ExtensionVersionNumber": self.extension_version_number,
        })
    }
}

#[derive(Default)]
struct State {
    applications: BTreeMap<String, Application>,
    deployment_strategies: BTreeMap<String, DeploymentStrategy>,
    extensions: BTreeMap<String, Extension>,
    extension_associations: BTreeMap<String, ExtensionAssociation>,
    /// moto's TaggingService: resource ARN → key/value map.
    tags: BTreeMap<String, BTreeMap<String, String>>,
    deletion_protection: Option<Value>,
}

fn state() -> store::Slot<State> {
    static SLOT: Lazy<store::Slot<State>> = Lazy::new(|| store::slot("appconfig"));
    SLOT.clone()
}

// ---------------------------------------------------------------------------
// Wire helpers
// ---------------------------------------------------------------------------

fn ok(body: Value) -> Response {
    Response::new(
        200,
        "application/json",
        serde_json::to_vec(&body).unwrap_or_default(),
    )
}

fn ok_empty() -> Response {
    ok(json!({}))
}

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

/// moto's `JsonRESTError` — always 400 for appconfig exception classes.
fn not_found(message: &str) -> Response {
    error(400, "ResourceNotFoundException", message)
}

fn app_not_found() -> Response {
    not_found("Application not found")
}

fn profile_not_found() -> Response {
    not_found("ConfigurationProfile not found")
}

fn version_not_found() -> Response {
    not_found("HostedConfigurationVersion not found")
}

fn environment_not_found() -> Response {
    not_found("Environment not found")
}

fn strategy_not_found() -> Response {
    not_found("DeploymentStrategy not found")
}

fn deployment_not_found() -> Response {
    not_found("Deployment not found")
}

fn extension_not_found() -> Response {
    not_found("Extension not found")
}

fn association_not_found() -> Response {
    not_found("ExtensionAssociation not found")
}

fn not_implemented(message: &str) -> Response {
    error(501, "NotImplemented", message)
}

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

/// moto's `mock_random.get_random_hex(n)`.
fn random_hex(len: usize) -> String {
    let raw = uuid::Uuid::new_v4().simple().to_string();
    raw.chars().take(len).collect()
}

fn app_arn(region: &str, account: u64, app_id: &str) -> String {
    format!(
        "arn:{partition}:appconfig:{region}:{account}:application/{app_id}",
        partition = partition(region)
    )
}

/// Payload response for hosted-version create/get: raw content body plus the
/// metadata headers botocore reads the shape members from.
fn version_payload_response(version: &HostedConfigurationVersion) -> Response {
    let content_type = version
        .content_type
        .clone()
        .unwrap_or_else(|| "application/octet-stream".to_string());
    let mut resp = Response::new(200, &content_type, version.content.clone());
    for (k, v) in version.response_headers() {
        resp.headers.push((k, v));
    }
    resp
}

// ---------------------------------------------------------------------------
// Param helpers
// ---------------------------------------------------------------------------

fn param_str<'a>(p: &'a Value, key: &str) -> Option<&'a str> {
    p.get(key).and_then(Value::as_str)
}

fn param_i64(p: &Value, key: &str) -> Option<i64> {
    p.get(key)
        .and_then(Value::as_i64)
        .or_else(|| p.get(key).and_then(Value::as_f64).map(|v| v as i64))
}

/// Tags member wire shape: `{"key": "value"}` map.
fn appconfig_tags(p: &Value) -> BTreeMap<String, String> {
    p.get("Tags")
        .and_then(Value::as_object)
        .map(|m| {
            m.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect()
        })
        .unwrap_or_default()
}

fn apply_tags(st: &mut State, arn: &str, tags: &BTreeMap<String, String>) {
    if tags.is_empty() {
        return;
    }
    let entry = st.tags.entry(arn.to_string()).or_default();
    for (k, v) in tags {
        entry.insert(k.clone(), v.clone());
    }
}

fn tag_keys_from_query(query_string: &str) -> Vec<String> {
    let map = crate::proto::query_map(query_string);
    map.get("tagKeys").cloned().unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Route helpers
// ---------------------------------------------------------------------------

fn split_segs(path: &str) -> Vec<&str> {
    path.trim_start_matches('/')
        .trim_end_matches('/')
        .split('/')
        .filter(|s| !s.is_empty())
        .collect()
}

fn template_matches(uri: &str, segs: &[&str]) -> bool {
    let tpl: Vec<&str> = uri.trim_start_matches('/').split('/').collect();
    tpl.len() == segs.len()
        && tpl
            .iter()
            .zip(segs)
            .all(|(t, s)| (t.starts_with('{') && t.ends_with('}')) || *t == *s)
}

fn wire_operation(req: &Request) -> Option<&'static str> {
    let segs = split_segs(&req.path);
    RULES
        .iter()
        .find(|(method, uri, _)| *method == req.method && template_matches(uri, &segs))
        .map(|(_, _, op)| *op)
}

fn seg_i64(segs: &[&str], idx: usize) -> Option<i64> {
    segs.get(idx)
        .map(|s| crate::proto::url_decode(s))
        .and_then(|s| s.parse().ok())
}

fn seg_str<'a>(segs: &[&'a str], idx: usize) -> &'a str {
    segs.get(idx).copied().unwrap_or("")
}

/// The ARN in `/tags/{ResourceArn}` — decoded after the split (botocore
/// percent-encodes the whole member, so the path stays single-segment).
fn tags_arn(path: &str) -> String {
    let after = path.split_once("/tags/").map(|(_, r)| r).unwrap_or("");
    crate::proto::url_decode(after)
}

/// Lowercased request headers botocore uses for members of
/// CreateHostedConfigurationVersion (Description / VersionLabel). Mirrors
/// moto's case-insensitive `self.headers.get(...)`.
fn headers_as_json(req: &Request) -> Map<String, Value> {
    let mut out = Map::new();
    for name in ["description", "content-type", "versionlabel"] {
        if let Some(v) = req.headers.get(name) {
            out.insert(name.to_string(), json!(v));
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "appconfig" {
        return None;
    }
    let Some(op) = wire_operation(req) else {
        return Some(not_implemented(&format!(
            "The {} action has not been implemented",
            req.operation
        )));
    };
    let body = crate::proto::parse_json_body(&req.body);
    let header_values = headers_as_json(req);
    let path = req.path.clone();

    let slot = state();
    let mut st = slot.write();
    let s = st.entry((req.account, req.region.clone())).or_default();
    Some(handle(s, op, &path, req, &body, &header_values))
}

fn handle(
    st: &mut State,
    op: &str,
    path: &str,
    req: &Request,
    p: &Value,
    h: &Map<String, Value>,
) -> Response {
    let segs = split_segs(path);
    match op {
        // --- Applications ---
        "CreateApplication" => {
            let tags = appconfig_tags(p);
            let id = random_hex(7);
            let arn = app_arn(&req.region, req.account, &id);
            let name = param_str(p, "Name").unwrap_or_default().to_string();
            let description = param_str(p, "Description").map(str::to_string);
            let app = Application {
                id: id.clone(),
                name,
                description,
                config_profiles: BTreeMap::new(),
                environments: BTreeMap::new(),
            };
            let described = app.describe();
            st.applications.insert(id, app);
            apply_tags(st, &arn, &tags);
            ok(described)
        }
        "GetApplication" => match st.applications.get(seg_str(&segs, 1)) {
            Some(app) => ok(app.describe()),
            None => app_not_found(),
        },
        "UpdateApplication" => {
            let app = match st.applications.get_mut(seg_str(&segs, 1)) {
                Some(app) => app,
                None => return app_not_found(),
            };
            if let Some(name) = param_str(p, "Name") {
                app.name = name.to_string();
            }
            if let Some(desc) = param_str(p, "Description") {
                app.description = Some(desc.to_string());
            }
            ok(app.describe())
        }
        "DeleteApplication" => {
            st.applications.remove(seg_str(&segs, 1));
            ok_empty()
        }
        "ListApplications" => {
            let items: Vec<Value> = st
                .applications
                .values()
                .map(Application::describe)
                .collect();
            ok(json!({ "Items": items }))
        }

        // --- Configuration profiles ---
        "CreateConfigurationProfile" => {
            let app_id = seg_str(&segs, 1).to_string();
            let tags = appconfig_tags(p);
            let id = random_hex(7);
            let arn = format!(
                "arn:{}:appconfig:{}:{}:application/{}/configurationprofile/{}",
                partition(&req.region),
                req.region,
                req.account,
                app_id,
                id
            );
            let profile = ConfigurationProfile {
                id: id.clone(),
                name: param_str(p, "Name").unwrap_or_default().to_string(),
                description: param_str(p, "Description").map(str::to_string),
                location_uri: param_str(p, "LocationUri").map(str::to_string),
                retrieval_role_arn: param_str(p, "RetrievalRoleArn").map(str::to_string),
                validators: p.get("Validators").filter(|v| v.is_array()).cloned(),
                kind: param_str(p, "Type").map(str::to_string),
                config_versions: BTreeMap::new(),
            };
            let Some(app) = st.applications.get_mut(&app_id) else {
                return app_not_found();
            };
            let described = profile.describe(&app_id);
            app.config_profiles.insert(id, profile);
            apply_tags(st, &arn, &tags);
            ok(described)
        }
        "GetConfigurationProfile" => {
            let app_id = seg_str(&segs, 1);
            let profile_id = seg_str(&segs, 3);
            match require_profile(st, app_id, profile_id) {
                Ok(profile) => ok(profile.describe(app_id)),
                Err(e) => e,
            }
        }
        "UpdateConfigurationProfile" => {
            let app_id = seg_str(&segs, 1).to_string();
            let profile_id = seg_str(&segs, 3).to_string();
            let Some(app) = st.applications.get_mut(&app_id) else {
                return app_not_found();
            };
            let Some(profile) = app.config_profiles.get_mut(&profile_id) else {
                return profile_not_found();
            };
            if let Some(name) = param_str(p, "Name") {
                profile.name = name.to_string();
            }
            if let Some(desc) = param_str(p, "Description") {
                profile.description = Some(desc.to_string());
            }
            if let Some(arn) = param_str(p, "RetrievalRoleArn") {
                profile.retrieval_role_arn = Some(arn.to_string());
            }
            if let Some(validators) = p.get("Validators").and_then(Value::as_array) {
                profile.validators = Some(Value::Array(validators.clone()));
            }
            ok(profile.describe(&app_id))
        }
        "DeleteConfigurationProfile" => {
            let app = match st.applications.get_mut(seg_str(&segs, 1)) {
                Some(app) => app,
                None => return app_not_found(),
            };
            app.config_profiles.remove(seg_str(&segs, 3));
            ok_empty()
        }
        "ListConfigurationProfiles" => {
            let app_id = seg_str(&segs, 1);
            match st.applications.get(app_id) {
                Some(app) => {
                    let items: Vec<Value> = app
                        .config_profiles
                        .values()
                        .map(|profile| profile.describe(app_id))
                        .collect();
                    ok(json!({ "Items": items }))
                }
                None => app_not_found(),
            }
        }

        // --- Hosted configuration versions ---
        "CreateHostedConfigurationVersion" => {
            let app_id = seg_str(&segs, 1);
            let profile_id = seg_str(&segs, 3);
            let Some(app) = st.applications.get_mut(app_id) else {
                return app_not_found();
            };
            let Some(profile) = app.config_profiles.get_mut(profile_id) else {
                return profile_not_found();
            };
            let version = profile.config_versions.keys().max().copied().unwrap_or(0) + 1;
            let ver = HostedConfigurationVersion {
                app_id: app_id.to_string(),
                config_id: profile_id.to_string(),
                version,
                description: h
                    .get("description")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                content: req.body.to_vec(),
                content_type: h
                    .get("content-type")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                version_label: h
                    .get("versionlabel")
                    .and_then(Value::as_str)
                    .map(str::to_string),
            };
            let resp = version_payload_response(&ver);
            profile.config_versions.insert(version, ver);
            resp
        }
        "GetHostedConfigurationVersion" => {
            let app_id = seg_str(&segs, 1);
            let profile_id = seg_str(&segs, 3);
            let Some(number) = seg_i64(&segs, 5) else {
                return version_not_found();
            };
            let ver = require_profile(st, app_id, profile_id).and_then(|profile| {
                profile
                    .config_versions
                    .get(&number)
                    .ok_or_else(version_not_found)
            });
            match ver {
                Ok(ver) => version_payload_response(ver),
                Err(e) => e,
            }
        }
        "DeleteHostedConfigurationVersion" => {
            let app_id = seg_str(&segs, 1);
            let profile_id = seg_str(&segs, 3);
            let Some(number) = seg_i64(&segs, 5) else {
                return version_not_found();
            };
            if let Err(e) = require_profile(st, app_id, profile_id) {
                return e;
            }
            if let Some(profile) = st
                .applications
                .get_mut(app_id)
                .and_then(|app| app.config_profiles.get_mut(profile_id))
            {
                profile.config_versions.remove(&number);
            }
            ok_empty()
        }
        "ListHostedConfigurationVersions" => {
            let app_id = seg_str(&segs, 1);
            let profile_id = seg_str(&segs, 3);
            match require_profile(st, app_id, profile_id) {
                Ok(profile) => {
                    let items: Vec<Value> = profile
                        .config_versions
                        .values()
                        .map(HostedConfigurationVersion::describe)
                        .collect();
                    ok(json!({ "Items": items }))
                }
                Err(e) => e,
            }
        }
        "ValidateConfiguration" => {
            let app_id = seg_str(&segs, 1);
            let profile_id = seg_str(&segs, 3);
            match require_profile(st, app_id, profile_id) {
                Ok(_) => ok_empty(),
                Err(e) => e,
            }
        }

        // --- Environments ---
        "CreateEnvironment" => {
            let app_id = seg_str(&segs, 1).to_string();
            let tags = appconfig_tags(p);
            let id = random_hex(7);
            let arn = format!(
                "arn:{}:appconfig:{}:{}:application/{}/environment/{}",
                partition(&req.region),
                req.region,
                req.account,
                app_id,
                id
            );
            let Some(app) = st.applications.get_mut(&app_id) else {
                return app_not_found();
            };
            let env = Environment {
                id: id.clone(),
                arn: arn.clone(),
                application_id: app_id.clone(),
                name: param_str(p, "Name").unwrap_or_default().to_string(),
                description: param_str(p, "Description").map(str::to_string),
                monitors: p
                    .get("Monitors")
                    .filter(|v| v.is_array())
                    .cloned()
                    .unwrap_or_else(|| json!([])),
                deployments: BTreeMap::new(),
            };
            let described = env.describe();
            app.environments.insert(id, env);
            apply_tags(st, &arn, &tags);
            ok(described)
        }
        "GetEnvironment" => {
            let app_id = seg_str(&segs, 1);
            let env_id = seg_str(&segs, 3);
            match require_env(st, app_id, env_id) {
                Ok(env) => ok(env.describe()),
                Err(e) => e,
            }
        }
        "UpdateEnvironment" => {
            let app_id = seg_str(&segs, 1).to_string();
            let env_id = seg_str(&segs, 3).to_string();
            let Some(app) = st.applications.get_mut(&app_id) else {
                return app_not_found();
            };
            let Some(env) = app.environments.get_mut(&env_id) else {
                return environment_not_found();
            };
            if let Some(name) = param_str(p, "Name") {
                env.name = name.to_string();
            }
            if let Some(desc) = param_str(p, "Description") {
                env.description = Some(desc.to_string());
            }
            if let Some(monitors) = p.get("Monitors").and_then(Value::as_array) {
                env.monitors = Value::Array(monitors.clone());
            }
            ok(env.describe())
        }
        "DeleteEnvironment" => {
            let app = match st.applications.get_mut(seg_str(&segs, 1)) {
                Some(app) => app,
                None => return app_not_found(),
            };
            app.environments.remove(seg_str(&segs, 3));
            ok_empty()
        }
        "ListEnvironments" => {
            let app_id = seg_str(&segs, 1);
            match st.applications.get(app_id) {
                Some(app) => {
                    let items: Vec<Value> = app
                        .environments
                        .values()
                        .map(Environment::describe)
                        .collect();
                    ok(json!({ "Items": items }))
                }
                None => app_not_found(),
            }
        }

        // --- Deployment strategies ---
        "CreateDeploymentStrategy" => {
            let tags = appconfig_tags(p);
            let id = random_hex(7);
            let arn = format!(
                "arn:{}:appconfig:{}:{}:deploymentstrategy/{}",
                partition(&req.region),
                req.region,
                req.account,
                id
            );
            let strategy = DeploymentStrategy {
                id: id.clone(),
                name: param_str(p, "Name").unwrap_or_default().to_string(),
                description: param_str(p, "Description").map(str::to_string),
                deployment_duration_in_minutes: param_i64(p, "DeploymentDurationInMinutes")
                    .unwrap_or(0),
                final_bake_time_in_minutes: param_i64(p, "FinalBakeTimeInMinutes").unwrap_or(0),
                growth_factor: p.get("GrowthFactor").and_then(Value::as_f64).unwrap_or(0.0),
                growth_type: param_str(p, "GrowthType").unwrap_or("LINEAR").to_string(),
                replicate_to: param_str(p, "ReplicateTo").unwrap_or("NONE").to_string(),
            };
            let described = strategy.describe();
            st.deployment_strategies.insert(id, strategy);
            apply_tags(st, &arn, &tags);
            ok(described)
        }
        "GetDeploymentStrategy" => match st.deployment_strategies.get(seg_str(&segs, 1)) {
            Some(strategy) => ok(strategy.describe()),
            None => strategy_not_found(),
        },
        "UpdateDeploymentStrategy" => {
            let Some(strategy) = st.deployment_strategies.get_mut(seg_str(&segs, 1)) else {
                return strategy_not_found();
            };
            if let Some(desc) = param_str(p, "Description") {
                strategy.description = Some(desc.to_string());
            }
            if let Some(v) = p
                .get("DeploymentDurationInMinutes")
                .and_then(Value::as_i64)
                .or_else(|| {
                    p.get("DeploymentDurationInMinutes")
                        .and_then(Value::as_f64)
                        .map(|f| f as i64)
                })
            {
                strategy.deployment_duration_in_minutes = v;
            }
            if let Some(v) = p
                .get("FinalBakeTimeInMinutes")
                .and_then(Value::as_i64)
                .or_else(|| {
                    p.get("FinalBakeTimeInMinutes")
                        .and_then(Value::as_f64)
                        .map(|f| f as i64)
                })
            {
                strategy.final_bake_time_in_minutes = v;
            }
            if let Some(v) = p.get("GrowthFactor").and_then(Value::as_f64) {
                strategy.growth_factor = v;
            }
            if let Some(v) = param_str(p, "GrowthType") {
                strategy.growth_type = v.to_string();
            }
            ok(strategy.describe())
        }
        "DeleteDeploymentStrategy" => {
            st.deployment_strategies.remove(seg_str(&segs, 1));
            ok_empty()
        }
        "ListDeploymentStrategies" => {
            let items: Vec<Value> = st
                .deployment_strategies
                .values()
                .map(DeploymentStrategy::describe)
                .collect();
            ok(json!({ "Items": items }))
        }

        // --- Deployments ---
        "StartDeployment" => {
            let app_id = seg_str(&segs, 1).to_string();
            let env_id = seg_str(&segs, 3).to_string();
            let strategy_id = param_str(p, "DeploymentStrategyId")
                .unwrap_or_default()
                .to_string();
            let profile_id = param_str(p, "ConfigurationProfileId")
                .unwrap_or_default()
                .to_string();
            let env_arn = match require_env(st, &app_id, &env_id) {
                Ok(env) => env.arn.clone(),
                Err(e) => return e,
            };
            let strategy = match st.deployment_strategies.get(&strategy_id) {
                Some(strategy) => strategy,
                None => return strategy_not_found(),
            };
            let strategy_fields = (
                strategy.deployment_duration_in_minutes,
                strategy.growth_type.clone(),
                strategy.growth_factor,
                strategy.final_bake_time_in_minutes,
            );
            let profile_fields = match require_profile(st, &app_id, &profile_id) {
                Ok(profile) => (profile.name.clone(), profile.location_uri.clone()),
                Err(e) => return e,
            };
            let number = st
                .applications
                .get(&app_id)
                .and_then(|app| app.environments.get(&env_id))
                .and_then(|env| env.deployments.keys().max().copied())
                .unwrap_or(0)
                + 1;
            let now = crate::proto::now_iso();
            let deployment = Deployment {
                application_id: app_id.clone(),
                environment_id: env_id.clone(),
                deployment_strategy_id: strategy_id,
                configuration_profile_id: profile_id,
                configuration_version: param_str(p, "ConfigurationVersion").map(str::to_string),
                description: param_str(p, "Description").map(str::to_string),
                kms_key_identifier: param_str(p, "KmsKeyIdentifier").map(str::to_string),
                deployment_number: number,
                configuration_name: profile_fields.0,
                configuration_location_uri: profile_fields.1,
                deployment_duration_in_minutes: strategy_fields.0,
                growth_type: strategy_fields.1,
                growth_factor: strategy_fields.2,
                final_bake_time_in_minutes: strategy_fields.3,
                state: "COMPLETE".to_string(),
                percentage_complete: 100.0,
                started_at: now.clone(),
                completed_at: now,
                version_label: None,
            };
            let described = deployment.describe();
            if let Some(app) = st.applications.get_mut(&app_id) {
                if let Some(env) = app.environments.get_mut(&env_id) {
                    env.deployments.insert(number, deployment);
                }
            }
            let tags = appconfig_tags(p);
            apply_tags(st, &env_arn, &tags);
            ok(described)
        }
        "GetDeployment" => {
            let app_id = seg_str(&segs, 1);
            let env_id = seg_str(&segs, 3);
            let Some(number) = seg_i64(&segs, 5) else {
                return deployment_not_found();
            };
            let dep = require_env(st, app_id, env_id).and_then(|env| {
                env.deployments
                    .get(&number)
                    .ok_or_else(deployment_not_found)
            });
            match dep {
                Ok(dep) => ok(dep.describe()),
                Err(e) => e,
            }
        }
        "StopDeployment" => {
            let app_id = seg_str(&segs, 1).to_string();
            let env_id = seg_str(&segs, 3).to_string();
            let Some(number) = seg_i64(&segs, 5) else {
                return deployment_not_found();
            };
            let Some(app) = st.applications.get_mut(&app_id) else {
                return app_not_found();
            };
            let Some(env) = app.environments.get_mut(&env_id) else {
                return environment_not_found();
            };
            let Some(dep) = env.deployments.get_mut(&number) else {
                return deployment_not_found();
            };
            dep.state = "ROLLED_BACK".to_string();
            ok(dep.describe())
        }
        "ListDeployments" => {
            let app_id = seg_str(&segs, 1);
            let env_id = seg_str(&segs, 3);
            match require_env(st, app_id, env_id) {
                Ok(env) => {
                    let items: Vec<Value> = env
                        .deployments
                        .values()
                        .map(Deployment::describe_summary)
                        .collect();
                    ok(json!({ "Items": items }))
                }
                Err(e) => e,
            }
        }

        // --- Extensions ---
        "CreateExtension" => {
            let tags = appconfig_tags(p);
            let id = random_hex(7);
            let arn = format!(
                "arn:{}:appconfig:{}:{}:extension/{}",
                partition(&req.region),
                req.region,
                req.account,
                id
            );
            let ext = Extension {
                id: id.clone(),
                arn: arn.clone(),
                name: param_str(p, "Name").unwrap_or_default().to_string(),
                description: param_str(p, "Description").map(str::to_string),
                actions: p
                    .get("Actions")
                    .filter(|v| v.is_object())
                    .cloned()
                    .unwrap_or_else(|| json!({})),
                parameters: p
                    .get("Parameters")
                    .filter(|v| v.is_object())
                    .cloned()
                    .unwrap_or_else(|| json!({})),
                version_number: 1,
            };
            let described = ext.describe();
            st.extensions.insert(id, ext);
            apply_tags(st, &arn, &tags);
            ok(described)
        }
        "GetExtension" => match st.extensions.get(seg_str(&segs, 1)) {
            Some(ext) => ok(ext.describe()),
            None => extension_not_found(),
        },
        "UpdateExtension" => {
            let Some(ext) = st.extensions.get_mut(seg_str(&segs, 1)) else {
                return extension_not_found();
            };
            if let Some(desc) = param_str(p, "Description") {
                ext.description = Some(desc.to_string());
            }
            if let Some(actions) = p.get("Actions").and_then(Value::as_object) {
                ext.actions = Value::Object(actions.clone());
            }
            if let Some(params) = p.get("Parameters").and_then(Value::as_object) {
                ext.parameters = Value::Object(params.clone());
            }
            ext.version_number += 1;
            ok(ext.describe())
        }
        "DeleteExtension" => {
            st.extensions.remove(seg_str(&segs, 1));
            ok_empty()
        }
        "ListExtensions" => {
            let items: Vec<Value> = st.extensions.values().map(Extension::describe).collect();
            ok(json!({ "Items": items }))
        }

        // --- Extension associations ---
        "CreateExtensionAssociation" => {
            let ext_id = param_str(p, "ExtensionIdentifier")
                .unwrap_or_default()
                .to_string();
            let Some(ext) = st.extensions.get(&ext_id) else {
                return extension_not_found();
            };
            let extension_arn = ext.arn.clone();
            let tags = appconfig_tags(p);
            let id = random_hex(7);
            let arn = format!(
                "arn:{}:appconfig:{}:{}:extensionassociation/{}",
                partition(&req.region),
                req.region,
                req.account,
                id
            );
            let assoc = ExtensionAssociation {
                id: id.clone(),
                arn: arn.clone(),
                extension_arn,
                resource_arn: param_str(p, "ResourceIdentifier")
                    .unwrap_or_default()
                    .to_string(),
                parameters: p
                    .get("Parameters")
                    .filter(|v| v.is_object())
                    .cloned()
                    .unwrap_or_else(|| json!({})),
                extension_version_number: p
                    .get("ExtensionVersionNumber")
                    .and_then(Value::as_i64)
                    .filter(|v| *v != 0)
                    .unwrap_or(1),
            };
            let described = assoc.describe();
            st.extension_associations.insert(id, assoc);
            apply_tags(st, &arn, &tags);
            ok(described)
        }
        "GetExtensionAssociation" => match st.extension_associations.get(seg_str(&segs, 1)) {
            Some(assoc) => ok(assoc.describe()),
            None => association_not_found(),
        },
        "UpdateExtensionAssociation" => {
            let Some(assoc) = st.extension_associations.get_mut(seg_str(&segs, 1)) else {
                return association_not_found();
            };
            if let Some(params) = p.get("Parameters").and_then(Value::as_object) {
                assoc.parameters = Value::Object(params.clone());
            }
            ok(assoc.describe())
        }
        "DeleteExtensionAssociation" => {
            st.extension_associations.remove(seg_str(&segs, 1));
            ok_empty()
        }
        "ListExtensionAssociations" => {
            let items: Vec<Value> = st
                .extension_associations
                .values()
                .map(ExtensionAssociation::describe)
                .collect();
            ok(json!({ "Items": items }))
        }

        // --- Deprecated GetConfiguration ---
        "GetConfiguration" => {
            let app_id = seg_str(&segs, 1);
            let config_id = seg_str(&segs, 5);
            match require_profile(st, app_id, config_id) {
                Ok(profile) => match profile.latest_version() {
                    Some(ver) => {
                        let content_type = ver
                            .content_type
                            .clone()
                            .unwrap_or_else(|| "application/octet-stream".to_string());
                        let mut resp = Response::new(200, &content_type, ver.content.clone());
                        resp.headers
                            .push(("Configuration-Version".to_string(), "1".to_string()));
                        resp
                    }
                    None => {
                        let mut resp =
                            Response::new(200, "application/octet-stream", Vec::<u8>::new());
                        resp.headers
                            .push(("Configuration-Version".to_string(), "1".to_string()));
                        resp
                    }
                },
                Err(e) => e,
            }
        }

        // --- Account settings ---
        "GetAccountSettings" => ok(json!({
            "DeletionProtection": st.deletion_protection.clone(),
        })),
        "UpdateAccountSettings" => {
            if let Some(v) = p.get("DeletionProtection").filter(|v| !v.is_null()) {
                st.deletion_protection = Some(v.clone());
            }
            ok(json!({
                "DeletionProtection": st.deletion_protection.clone(),
            }))
        }

        // --- Tags ---
        "ListTagsForResource" => ok(json!({
            "Tags": tags_json(st, &tags_arn(path)),
        })),
        "TagResource" => {
            let arn = tags_arn(path);
            let tags = appconfig_tags(p);
            apply_tags(st, &arn, &tags);
            ok_empty()
        }
        "UntagResource" => {
            let arn = tags_arn(path);
            let keys = tag_keys_from_query(&req.query_string);
            if let Some(entry) = st.tags.get_mut(&arn) {
                for key in keys {
                    entry.remove(&key);
                }
            }
            ok_empty()
        }

        _ => not_implemented(&format!(
            "The {} action has not been implemented",
            req.operation
        )),
    }
}

// ---------------------------------------------------------------------------
// State lookups (moto's backend method semantics, translated)
// ---------------------------------------------------------------------------

fn require_env<'a>(st: &'a State, app_id: &str, env_id: &str) -> Result<&'a Environment, Response> {
    st.applications
        .get(app_id)
        .ok_or_else(app_not_found)
        .and_then(|app| {
            app.environments
                .get(env_id)
                .ok_or_else(environment_not_found)
        })
}

fn require_profile<'a>(
    st: &'a State,
    app_id: &str,
    profile_id: &str,
) -> Result<&'a ConfigurationProfile, Response> {
    st.applications
        .get(app_id)
        .ok_or_else(app_not_found)
        .and_then(|app| {
            app.config_profiles
                .get(profile_id)
                .ok_or_else(profile_not_found)
        })
}

fn tags_json(st: &State, arn: &str) -> Value {
    match st.tags.get(arn) {
        Some(entry) => {
            let mut map = Map::new();
            for (k, v) in entry {
                map.insert(k.clone(), json!(v));
            }
            Value::Object(map)
        }
        None => json!({}),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::collections::HashMap;

    fn req(method: &str, path: &str, body: Value) -> Request {
        let (path, query_string) = match path.split_once('?') {
            Some((p, q)) => (p.to_string(), q.to_string()),
            None => (path.to_string(), String::new()),
        };
        Request {
            service: "appconfig".into(),
            operation: String::new(),
            method: method.into(),
            path,
            query_string,
            headers: HashMap::new(),
            body: Bytes::from(serde_json::to_vec(&body).unwrap()),
            account: 123456789001,
            region: "us-east-1".into(),
        }
    }

    fn raw_req(method: &str, path: &str, content: &[u8]) -> Request {
        let mut r = req(method, path, json!({}));
        r.body = Bytes::copy_from_slice(content);
        r
    }

    fn json_body(resp: Response) -> Value {
        serde_json::from_slice(&resp.body).unwrap()
    }

    #[test]
    fn application_lifecycle() {
        let created =
            json_body(dispatch(&req("POST", "/applications", json!({"Name": "appA"}))).unwrap());
        let app_id = created["Id"].as_str().unwrap().to_string();
        assert_eq!(app_id.chars().count(), 7);
        assert_eq!(created["Name"], "appA");

        let got = json_body(
            dispatch(&req("GET", &format!("/applications/{app_id}"), json!({}))).unwrap(),
        );
        assert_eq!(got["Id"], app_id.as_str());

        let updated = json_body(
            dispatch(&req(
                "PATCH",
                &format!("/applications/{app_id}"),
                json!({"Name": "appB"}),
            ))
            .unwrap(),
        );
        assert_eq!(updated["Name"], "appB");

        let list = json_body(dispatch(&req("GET", "/applications", json!({}))).unwrap());
        assert!(list["Items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|a| a["Id"] == app_id.as_str()));

        assert_eq!(
            dispatch(&req(
                "DELETE",
                &format!("/applications/{app_id}"),
                json!({})
            ))
            .unwrap()
            .status,
            200
        );
        let miss = dispatch(&req("GET", &format!("/applications/{app_id}"), json!({}))).unwrap();
        assert_eq!(miss.status, 400);
        let miss_body = serde_json::from_slice::<Value>(&miss.body).unwrap();
        assert_eq!(miss_body["__type"], "ResourceNotFoundException");
        assert_eq!(miss_body["message"], "Application not found");
    }

    #[test]
    fn hosted_versions_increment() {
        let app = json_body(dispatch(&req("POST", "/applications", json!({"Name": "a"}))).unwrap());
        let app_id = app["Id"].as_str().unwrap().to_string();
        let profile = json_body(
            dispatch(&req(
                "POST",
                &format!("/applications/{app_id}/configurationprofiles"),
                json!({"Name": "p", "LocationUri": "hosted"}),
            ))
            .unwrap(),
        );
        let profile_id = profile["Id"].as_str().unwrap().to_string();

        let mut r1 = raw_req(
            "POST",
            &format!("/applications/{app_id}/configurationprofiles/{profile_id}/hostedconfigurationversions"),
            b"{\"k\": 1}",
        );
        r1.headers
            .insert("content-type".into(), "application/json".into());
        r1.headers.insert("description".into(), "first".into());
        // The wire carries the content raw; metadata rides headers (moto).
        let resp1 = dispatch(&r1).unwrap();
        assert_eq!(resp1.body, b"{\"k\": 1}".to_vec());
        let header_of = |resp: &Response, name: &str| {
            resp.headers
                .iter()
                .find(|(k, _)| k == name)
                .map(|(_, v)| v.clone())
        };
        assert_eq!(header_of(&resp1, "Version-Number").as_deref(), Some("1"));
        assert_eq!(
            header_of(&resp1, "Application-Id").as_deref(),
            Some(app_id.as_str())
        );
        assert_eq!(
            header_of(&resp1, "Configuration-Profile-Id").as_deref(),
            Some(profile_id.as_str())
        );

        let r2 = raw_req(
            "POST",
            &format!("/applications/{app_id}/configurationprofiles/{profile_id}/hostedconfigurationversions"),
            b"{\"k\": 2}",
        );
        let mut r2 = r2;
        r2.headers
            .insert("content-type".into(), "application/json".into());
        let resp2 = dispatch(&r2).unwrap();
        assert_eq!(header_of(&resp2, "Version-Number").as_deref(), Some("2"));

        let listed = json_body(
            dispatch(&req(
                "GET",
                &format!("/applications/{app_id}/configurationprofiles/{profile_id}/hostedconfigurationversions"),
                json!({}),
            ))
            .unwrap(),
        );
        assert_eq!(listed["Items"].as_array().unwrap().len(), 2);

        let get_v1 = dispatch(
            &req(
                "GET",
                &format!("/applications/{app_id}/configurationprofiles/{profile_id}/hostedconfigurationversions/1"),
                json!({}),
            ),
        )
        .unwrap();
        assert_eq!(get_v1.body, b"{\"k\": 1}".to_vec());
        assert!(get_v1
            .headers
            .iter()
            .any(|(k, v)| k == "Content-Type" && v == "application/json"));

        dispatch(
            &req(
                "DELETE",
                &format!("/applications/{app_id}/configurationprofiles/{profile_id}/hostedconfigurationversions/1"),
                json!({}),
            )
        );
        let gone = dispatch(
            &req(
                "GET",
                &format!("/applications/{app_id}/configurationprofiles/{profile_id}/hostedconfigurationversions/1"),
                json!({}),
            ),
        )
        .unwrap();
        assert_eq!(gone.status, 400);
    }

    #[test]
    fn tags_round_trip() {
        let app = json_body(
            dispatch(&req(
                "POST",
                "/applications",
                json!({"Name": "a", "Tags": {"env": "dev"}}),
            ))
            .unwrap(),
        );
        let app_id = app["Id"].as_str().unwrap().to_string();
        let arn_val = format!("arn:aws:appconfig:us-east-1:123456789001:application/{app_id}");
        let arn = urlencoding::encode(&arn_val).to_string();

        let listed = json_body(dispatch(&req("GET", &format!("/tags/{arn}"), json!({}))).unwrap());
        assert_eq!(listed["Tags"]["env"], "dev");

        dispatch(&req(
            "POST",
            &format!("/tags/{arn}"),
            json!({"Tags": {"team": "x"}}),
        ));
        let listed = json_body(dispatch(&req("GET", &format!("/tags/{arn}"), json!({}))).unwrap());
        assert_eq!(listed["Tags"]["team"], "x");

        dispatch(&req(
            "DELETE",
            &format!("/tags/{arn}?tagKeys=env"),
            json!({}),
        ));
        let listed = json_body(dispatch(&req("GET", &format!("/tags/{arn}"), json!({}))).unwrap());
        assert!(listed["Tags"].get("env").is_none());
        assert_eq!(listed["Tags"]["team"], "x");
    }

    #[test]
    fn deployment_lifecycle() {
        let app = json_body(dispatch(&req("POST", "/applications", json!({"Name": "a"}))).unwrap());
        let app_id = app["Id"].as_str().unwrap().to_string();
        let profile = json_body(
            dispatch(&req(
                "POST",
                &format!("/applications/{app_id}/configurationprofiles"),
                json!({"Name": "p", "LocationUri": "hosted"}),
            ))
            .unwrap(),
        );
        let profile_id = profile["Id"].as_str().unwrap().to_string();
        let env = json_body(
            dispatch(&req(
                "POST",
                &format!("/applications/{app_id}/environments"),
                json!({"Name": "e"}),
            ))
            .unwrap(),
        );
        let env_id = env["Id"].as_str().unwrap().to_string();
        let strategy = json_body(
            dispatch(&req(
                "POST",
                "/deploymentstrategies",
                json!({"Name": "s", "DeploymentDurationInMinutes": 3, "GrowthFactor": 50.0}),
            ))
            .unwrap(),
        );
        let strategy_id = strategy["Id"].as_str().unwrap().to_string();

        let started = json_body(
            dispatch(&req(
                "POST",
                &format!("/applications/{app_id}/environments/{env_id}/deployments"),
                json!({
                    "DeploymentStrategyId": strategy_id,
                    "ConfigurationProfileId": profile_id,
                    "ConfigurationVersion": "1",
                }),
            ))
            .unwrap(),
        );
        assert_eq!(started["State"], "COMPLETE");
        assert_eq!(started["GrowthType"], "LINEAR");
        assert_eq!(started["DeploymentNumber"], 1);
        assert_eq!(started["ConfigurationName"], "p");
        assert_eq!(started["PercentageComplete"], 100.0);

        let listed = json_body(
            dispatch(&req(
                "GET",
                &format!("/applications/{app_id}/environments/{env_id}/deployments"),
                json!({}),
            ))
            .unwrap(),
        );
        assert_eq!(listed["Items"].as_array().unwrap().len(), 1);

        let stopped = json_body(
            dispatch(&req(
                "DELETE",
                &format!("/applications/{app_id}/environments/{env_id}/deployments/1"),
                json!({}),
            ))
            .unwrap(),
        );
        assert_eq!(stopped["State"], "ROLLED_BACK");
    }
}
