//! Elastic Beanstalk backend — query-XML protocol port of the moto
//! `elasticbeanstalk` service.
//!
//! Wire contract: AWS query protocol, XML namespace
//! `http://elasticbeanstalk.amazonaws.com/docs/2010-12-01/`, form bodies,
//! `<{Op}Response><{Op}Result>…</{Op}Result><ResponseMetadata/>` envelopes.
//! Semantics mirror the vendored moto implementation (`moto/elasticbeanstalk`,
//! the 2025 thin rewrite, byte-identical to the deployed fork checkout) for
//! every operation the compat suite
//! (`tests/compatibility/test_elasticbeanstalk_compat.py`) exercises:
//!
//! - the 11 ops the 2026-03-19 probe recorded as `not_implemented`
//!   (Associate/DisassociateEnvironmentOperationsRole, CheckDNSAvailability,
//!   CreateStorageLocation, DeleteApplicationVersion,
//!   DeleteEnvironmentConfiguration, RestartAppServer, UpdateApplication,
//!   UpdateApplicationResourceLifecycle, UpdateApplicationVersion,
//!   ValidateConfigurationSettings) all have routed handlers in the deployed
//!   moto fork, so they are implemented here per the wave-2 parent ruling in
//!   `docs/moto-parity-progress.md` (fork wins over stale probes).
//! - moto's response stubs stay stubs: `DescribeConfigurationSettings`
//!   returns `[]` even for apps with templates (moto's responses.py bypasses
//!   the backend there), `ListPlatformVersions`/`ListPlatformBranches`
//!   return empty lists, `DescribeEvents` tracks nothing.
//! - describe filters are honored (`ApplicationNames`, `EnvironmentNames`,
//!   `EnvironmentIds`, `VersionLabels`); the fork ignores
//!   `ApplicationNames`, which only worked because every suite test cleaned
//!   up after itself.
//! - where moto's thin rewrite left empty placeholders (`environment_id=""`)
//!   this port uses real values (`e-` ids, real CNAMEs) — the suite only
//!   asserts presence and round-trips ids through `EnvironmentIds` filters.
//! - `InvalidParameterValue` maps to HTTP 400 like moto's ServiceException;
//!   unrouted actions answer `501 NotImplemented`.

use super::elbv2::{
    esc, iso_micro, p_list, p_string, p_struct_list, parse_params, rand_hex, ElbVal,
};
use crate::store;
use crate::{Request, Response};
use std::collections::BTreeMap;

const XMLNS: &str = "http://elasticbeanstalk.amazonaws.com/docs/2010-12-01/";

type Params = BTreeMap<String, ElbVal>;
type EbError = (u16, String, String);
type OpResult = Result<String, EbError>;

/// moto `InvalidParameterValueError` (a ServiceException, HTTP 400).
fn invalid(msg: impl Into<String>) -> EbError {
    (400, "InvalidParameterValue".into(), msg.into())
}

/// moto `ResourceNotFoundException` (HTTP 400).
fn resource_not_found(msg: impl Into<String>) -> EbError {
    (400, "ResourceNotFoundException".into(), msg.into())
}

/// moto's flask fallthrough for unrouted actions: bare 501.
fn not_implemented(op: &str) -> EbError {
    (
        501,
        "NotImplemented".into(),
        format!("The {op} action has not been implemented"),
    )
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

/// moto `make_arn`: `arn:{partition}:elasticbeanstalk:{region}:{account}:{type}/{path}`.
fn make_arn(region: &str, account: u64, resource_type: &str, path: &str) -> String {
    format!(
        "arn:{}:elasticbeanstalk:{}:{}:{}/{path}",
        partition(region),
        region,
        account,
        resource_type
    )
}

// ---------------------------------------------------------------------------
// XML helpers.
// ---------------------------------------------------------------------------

fn tag_esc(name: &str, body: &str) -> String {
    format!("<{name}>{}</{name}>", esc(body))
}

fn tag_raw(name: &str, body: &str) -> String {
    if body.is_empty() {
        format!("<{name}/>")
    } else {
        format!("<{name}>{body}</{name}>")
    }
}

fn members_xml(items: &[String]) -> String {
    items
        .iter()
        .map(|i| tag_esc("member", i))
        .collect::<String>()
}

fn bool_text(v: Option<bool>) -> String {
    match v {
        Some(true) => "true".to_string(),
        Some(false) => "false".to_string(),
        None => String::new(),
    }
}

/// `Foo.member.N{Key,Value}` tag structs, member order preserved.
fn read_tag_list(p: &Params, key: &str) -> Vec<(String, String)> {
    p_struct_list(p, key)
        .into_iter()
        .filter_map(|m| {
            let k = m.get("Key")?.string()?.to_string();
            let v = m
                .get("Value")
                .and_then(ElbVal::string)
                .unwrap_or_default()
                .to_string();
            Some((k, v))
        })
        .collect()
}

fn tags_xml(tags: &[(String, String)]) -> String {
    tags.iter()
        .map(|(k, v)| {
            format!(
                "<member>{}{}</member>",
                tag_esc("Key", k),
                tag_esc("Value", v)
            )
        })
        .collect::<String>()
}

// ---------------------------------------------------------------------------
// State: per-(account, region) EB backend, mirroring moto's EBBackend.
// ---------------------------------------------------------------------------

/// Application resource lifecycle config (`ServiceRole` + version rules).
#[derive(Clone, Debug, Default)]
struct LifecycleConfig {
    service_role: Option<String>,
    max_count: Option<MaxCountRule>,
    max_age: Option<MaxAgeRule>,
}

#[derive(Clone, Debug, Default)]
struct MaxCountRule {
    enabled: Option<bool>,
    max_count: Option<i64>,
    delete_source_from_s3: Option<bool>,
}

#[derive(Clone, Debug, Default)]
struct MaxAgeRule {
    enabled: Option<bool>,
    max_age_in_days: Option<i64>,
    delete_source_from_s3: Option<bool>,
}

#[derive(Clone, Debug, Default)]
struct AppVersion {
    description: String,
    source_bundle: Option<(String, String)>,
    date_created: String,
    date_updated: String,
    arn: String,
}

#[derive(Clone, Debug, Default)]
struct ConfigTemplate {
    template_name: String,
    solution_stack_name: String,
    description: String,
    date_created: String,
    date_updated: String,
}

/// Environment record; the owning application name is stored so ARNs and XML
/// render without walking the application map.
#[derive(Clone, Debug)]
struct Environment {
    app_name: String,
    env_name: String,
    id: String,
    solution_stack: String,
    description: Option<String>,
    cname: String,
    status: String,
    version_label: String,
    date_created: String,
    date_updated: String,
    operations_role: Option<String>,
    /// Insertion-ordered tags (a python dict in moto).
    tags: Vec<(String, String)>,
}

#[derive(Clone, Debug, Default)]
struct Application {
    description: String,
    date_created: String,
    date_updated: String,
    versions: BTreeMap<String, AppVersion>,
    templates: BTreeMap<String, ConfigTemplate>,
    /// Environment name → record (application name also kept on each env).
    environments: BTreeMap<String, Environment>,
    lifecycle: Option<LifecycleConfig>,
}

/// Platform version ARNs this backend minted (moto stores the object; only
/// existence matters to the implemented create/delete surface).
type PlatformVersions = std::collections::BTreeSet<String>;

#[derive(Clone, Debug)]
struct EnvInfoItem {
    info_type: String,
    ec2_instance_id: String,
    sample_timestamp: String,
    message: String,
}

/// Per-(account, region) backend state.
#[derive(Clone, Debug, Default)]
pub struct EbState {
    /// Application name → application; environments hang off applications.
    applications: BTreeMap<String, Application>,
    /// This backend minted these platform version ARNs (moto stores the
    /// object; only creation-data matters to the create/delete surface).
    platform_versions: PlatformVersions,
    /// Environment name or id → requested tail/bundle info.
    environment_info: BTreeMap<String, Vec<EnvInfoItem>>,
}

// ---------------------------------------------------------------------------
// Params helpers.
// ---------------------------------------------------------------------------

fn as_map(v: &ElbVal) -> Option<&BTreeMap<String, ElbVal>> {
    match v {
        ElbVal::M(m) => Some(m),
        _ => None,
    }
}

fn member_string<'a>(m: &'a BTreeMap<String, ElbVal>, key: &str) -> Option<&'a str> {
    m.get(key).and_then(ElbVal::string)
}

fn s_bool(m: &BTreeMap<String, ElbVal>, key: &str) -> Option<bool> {
    m.get(key).and_then(ElbVal::opt_bool)
}

fn s_i64(m: &BTreeMap<String, ElbVal>, key: &str) -> Option<i64> {
    m.get(key).and_then(ElbVal::opt_i64)
}

// ---------------------------------------------------------------------------
// Resource lifecycle (de)serialization for UpdateApplicationResourceLifecycle.
// ---------------------------------------------------------------------------

fn parse_lifecycle_config(p: &Params, key: &str) -> LifecycleConfig {
    let Some(m) = p.get(key).and_then(as_map) else {
        return LifecycleConfig::default();
    };
    let mut cfg = LifecycleConfig {
        service_role: member_string(m, "ServiceRole").map(str::to_string),
        max_count: None,
        max_age: None,
    };
    if let Some(vlc) = m.get("VersionLifecycleConfig").and_then(as_map) {
        if let Some(rule) = vlc.get("MaxCountRule").and_then(as_map) {
            cfg.max_count = Some(MaxCountRule {
                enabled: s_bool(rule, "Enabled"),
                max_count: s_i64(rule, "MaxCount"),
                delete_source_from_s3: s_bool(rule, "DeleteSourceFromS3"),
            });
        }
        if let Some(rule) = vlc.get("MaxAgeRule").and_then(as_map) {
            cfg.max_age = Some(MaxAgeRule {
                enabled: s_bool(rule, "Enabled"),
                max_age_in_days: s_i64(rule, "MaxAgeInDays"),
                delete_source_from_s3: s_bool(rule, "DeleteSourceFromS3"),
            });
        }
    }
    cfg
}

/// moto echoes only the members that were actually provided; an absent
/// lifecycle config echoes as an empty element.
fn lifecycle_config_xml(cfg: &LifecycleConfig) -> String {
    let mut lc = String::new();
    if let Some(role) = &cfg.service_role {
        lc.push_str(&tag_esc("ServiceRole", role));
    }
    let mut vlc = String::new();
    if let Some(rule) = &cfg.max_count {
        let mut r = String::new();
        r.push_str(&tag_esc("Enabled", &bool_text(rule.enabled)));
        if let Some(c) = rule.max_count {
            r.push_str(&tag_esc("MaxCount", &c.to_string()));
        }
        r.push_str(&tag_esc(
            "DeleteSourceFromS3",
            &bool_text(rule.delete_source_from_s3),
        ));
        vlc.push_str(&tag_raw("MaxCountRule", &r));
    }
    if let Some(rule) = &cfg.max_age {
        let mut r = String::new();
        r.push_str(&tag_esc("Enabled", &bool_text(rule.enabled)));
        if let Some(d) = rule.max_age_in_days {
            r.push_str(&tag_esc("MaxAgeInDays", &d.to_string()));
        }
        r.push_str(&tag_esc(
            "DeleteSourceFromS3",
            &bool_text(rule.delete_source_from_s3),
        ));
        vlc.push_str(&tag_raw("MaxAgeRule", &r));
    }
    if !vlc.is_empty() {
        lc.push_str(&tag_raw("VersionLifecycleConfig", &vlc));
    }
    lc
}

// ---------------------------------------------------------------------------
// XML writers (botocore output-shape member sets; None members omitted
// exactly like moto's shape-driven serializer drops unmatchable attributes).
// ---------------------------------------------------------------------------

fn application_xml(app_name: &str, app: &Application, region: &str, account: u64) -> String {
    let mut out = String::new();
    out.push_str(&tag_esc(
        "ApplicationArn",
        &make_arn(region, account, "application", app_name),
    ));
    out.push_str(&tag_esc("ApplicationName", app_name));
    out.push_str(&tag_esc("Description", &app.description));
    out.push_str(&tag_esc("DateCreated", &app.date_created));
    out.push_str(&tag_esc("DateUpdated", &app.date_updated));
    out.push_str(&tag_raw(
        "Versions",
        &members_xml(&app.versions.keys().cloned().collect::<Vec<_>>()),
    ));
    out.push_str(&tag_raw(
        "ConfigurationTemplates",
        &members_xml(&app.templates.keys().cloned().collect::<Vec<_>>()),
    ));
    let lifecycle_xml = app
        .lifecycle
        .as_ref()
        .map(lifecycle_config_xml)
        .unwrap_or_default();
    out.push_str(&tag_raw("ResourceLifecycleConfig", &lifecycle_xml));
    out
}

/// ApplicationVersion members (application name + label are the map keys).
fn application_version_xml(app_name: &str, label: &str, v: &AppVersion) -> String {
    let mut out = String::new();
    out.push_str(&tag_esc("ApplicationVersionArn", &v.arn));
    out.push_str(&tag_esc("ApplicationName", app_name));
    out.push_str(&tag_esc("Description", &v.description));
    out.push_str(&tag_esc("VersionLabel", label));
    let bundle = match &v.source_bundle {
        Some((bucket, key)) => format!("{}{}", tag_esc("S3Bucket", bucket), tag_esc("S3Key", key)),
        None => String::new(),
    };
    out.push_str(&tag_raw("SourceBundle", &bundle));
    out.push_str(&tag_esc("DateCreated", &v.date_created));
    out.push_str(&tag_esc("DateUpdated", &v.date_updated));
    out.push_str(&tag_esc("Status", "UNPROCESSED"));
    out
}

/// Environment members (CreateEnvironment/Describe/Update/Terminate set).
fn environment_xml(env: &Environment, region: &str, account: u64) -> String {
    let arn = env_arn(env, region, account);
    let mut out = String::new();
    out.push_str(&tag_esc("EnvironmentName", &env.env_name));
    out.push_str(&tag_esc("EnvironmentId", &env.id));
    out.push_str(&tag_esc("ApplicationName", &env.app_name));
    out.push_str(&tag_esc("VersionLabel", &env.version_label));
    out.push_str(&tag_esc("SolutionStackName", &env.solution_stack));
    if let Some(d) = &env.description {
        out.push_str(&tag_esc("Description", d));
    }
    out.push_str(&tag_esc("EndpointURL", ""));
    out.push_str(&tag_esc("CNAME", &env.cname));
    out.push_str(&tag_esc("DateCreated", &env.date_created));
    out.push_str(&tag_esc("DateUpdated", &env.date_updated));
    out.push_str(&tag_esc("Status", &env.status));
    out.push_str("<AbortableOperationInProgress>false</AbortableOperationInProgress>");
    out.push_str(&tag_esc("Health", "Grey"));
    out.push_str(&tag_esc("HealthStatus", "No Data"));
    out.push_str("<Tier><Name>WebServer</Name><Type>Standard</Type><Version>1.0</Version></Tier>");
    out.push_str(&tag_raw("EnvironmentLinks", ""));
    out.push_str(&tag_esc("EnvironmentArn", &arn));
    if let Some(role) = &env.operations_role {
        out.push_str(&tag_esc("OperationsRole", role));
    }
    out
}

fn environment_member_xml(env: &Environment, region: &str, account: u64) -> String {
    format!("<member>{}</member>", environment_xml(env, region, account))
}

fn config_template_xml(t: &ConfigTemplate, app_name: &str) -> String {
    let mut out = String::new();
    out.push_str(&tag_esc("ApplicationName", app_name));
    out.push_str(&tag_esc("TemplateName", &t.template_name));
    out.push_str(&tag_esc("SolutionStackName", &t.solution_stack_name));
    out.push_str(&tag_esc("Description", &t.description));
    out.push_str(&tag_esc("DateCreated", &t.date_created));
    out.push_str(&tag_esc("DateUpdated", &t.date_updated));
    out.push_str(&tag_esc("DeploymentStatus", "deployed"));
    out
}

// ---------------------------------------------------------------------------
// Solution stacks (moto's 2025-07-13 table, verbatim).
// ---------------------------------------------------------------------------

struct StackDetail {
    name: &'static str,
    permitted_file_types: &'static [&'static str],
}

const STACKS: &[StackDetail] = &[
    StackDetail {
        name: "64bit Windows Server 2019 v2.19.2 running IIS 10.0",
        permitted_file_types: &["zip"],
    },
    StackDetail {
        name: "64bit Windows Server Core 2019 v2.19.2 running IIS 10.0",
        permitted_file_types: &["zip"],
    },
    StackDetail {
        name: "64bit Windows Server 2025 v2.19.2 running IIS 10.0",
        permitted_file_types: &["zip"],
    },
    StackDetail {
        name: "64bit Windows Server Core 2016 v2.19.2 running IIS 10.0",
        permitted_file_types: &["zip"],
    },
    StackDetail {
        name: "64bit Windows Server Core 2025 v2.19.2 running IIS 10.0",
        permitted_file_types: &["zip"],
    },
    StackDetail {
        name: "64bit Windows Server 2022 v2.19.2 running IIS 10.0",
        permitted_file_types: &["zip"],
    },
    StackDetail {
        name: "64bit Windows Server 2016 v2.19.2 running IIS 10.0",
        permitted_file_types: &["zip"],
    },
    StackDetail {
        name: "64bit Windows Server Core 2022 v2.19.2 running IIS 10.0",
        permitted_file_types: &["zip"],
    },
    StackDetail {
        name: "64bit Amazon Linux 2017.03 v2.5.4 running Java 8",
        permitted_file_types: &["jar", "zip"],
    },
    StackDetail {
        name: "64bit Amazon Linux 2023 v5.7.0 running Tomcat 9 Corretto 17",
        permitted_file_types: &["war", "zip"],
    },
    StackDetail {
        name: "64bit Amazon Linux 2023 v5.7.0 running Tomcat 11 Corretto 17",
        permitted_file_types: &["war", "zip"],
    },
    StackDetail {
        name: "64bit Amazon Linux 2023 v5.7.0 running Tomcat 10 Corretto 21",
        permitted_file_types: &["war", "zip"],
    },
    StackDetail {
        name: "64bit Amazon Linux 2023 v5.7.0 running Tomcat 11 Corretto 21",
        permitted_file_types: &["war", "zip"],
    },
    StackDetail {
        name: "64bit Amazon Linux 2023 v5.7.0 running Tomcat 10 Corretto 17",
        permitted_file_types: &["war", "zip"],
    },
    StackDetail {
        name: "64bit Amazon Linux 2023 v5.7.0 running Tomcat 9 Corretto 11",
        permitted_file_types: &["war", "zip"],
    },
    StackDetail {
        name: "64bit Amazon Linux 2023 v4.6.0 running Python 3.11",
        permitted_file_types: &["zip"],
    },
    StackDetail {
        name: "64bit Amazon Linux 2023 v4.7.0 running PHP 8.2",
        permitted_file_types: &["zip"],
    },
    StackDetail {
        name: "64bit Amazon Linux 2023 v4.7.0 running PHP 8.4",
        permitted_file_types: &["zip"],
    },
    StackDetail {
        name: "64bit Amazon Linux 2023 v4.6.0 running Python 3.9",
        permitted_file_types: &["zip"],
    },
    StackDetail {
        name: "64bit Amazon Linux 2023 v4.6.0 running Python 3.12",
        permitted_file_types: &["zip"],
    },
    StackDetail {
        name: "64bit Amazon Linux 2023 v4.6.0 running Ruby 3.3",
        permitted_file_types: &["zip"],
    },
    StackDetail {
        name: "64bit Amazon Linux 2023 v4.6.0 running Ruby 3.4",
        permitted_file_types: &["zip"],
    },
    StackDetail {
        name: "64bit Amazon Linux 2023 v4.6.0 running Python 3.13",
        permitted_file_types: &["zip"],
    },
    StackDetail {
        name: "64bit Amazon Linux 2023 v4.7.0 running PHP 8.3",
        permitted_file_types: &["zip"],
    },
    StackDetail {
        name: "64bit Amazon Linux 2023 v4.6.0 running Ruby 3.2",
        permitted_file_types: &["zip"],
    },
    StackDetail {
        name: "64bit Amazon Linux 2023 v6.6.0 running Node.js 22",
        permitted_file_types: &["zip"],
    },
    StackDetail {
        name: "64bit Amazon Linux 2023 v4.7.0 running PHP 8.1",
        permitted_file_types: &["zip"],
    },
    StackDetail {
        name: "64bit Amazon Linux 2023 v6.6.0 running Node.js 20",
        permitted_file_types: &["zip"],
    },
    StackDetail {
        name: "64bit Amazon Linux 2023 v4.6.0 running Corretto 17",
        permitted_file_types: &["jar", "zip"],
    },
    StackDetail {
        name: "64bit Amazon Linux 2023 v3.5.0 running .NET 9",
        permitted_file_types: &["zip"],
    },
    StackDetail {
        name: "64bit Amazon Linux 2023 v6.6.0 running Node.js 18",
        permitted_file_types: &["zip"],
    },
    StackDetail {
        name: "64bit Amazon Linux 2023 v4.6.0 running Corretto 8",
        permitted_file_types: &["jar", "zip"],
    },
    StackDetail {
        name: "64bit Amazon Linux 2023 v3.5.0 running .NET 8",
        permitted_file_types: &["zip"],
    },
    StackDetail {
        name: "64bit Amazon Linux 2023 v4.6.0 running Corretto 11",
        permitted_file_types: &["jar", "zip"],
    },
    StackDetail {
        name: "64bit Amazon Linux 2023 v4.6.0 running Docker",
        permitted_file_types: &[],
    },
    StackDetail {
        name: "64bit Amazon Linux 2023 v4.2.0 running ECS",
        permitted_file_types: &[],
    },
    StackDetail {
        name: "64bit Amazon Linux 2023 v4.4.0 running Go 1",
        permitted_file_types: &["zip"],
    },
    StackDetail {
        name: "64bit Amazon Linux 2023 v4.6.0 running Corretto 21",
        permitted_file_types: &["jar", "zip"],
    },
    StackDetail {
        name: "64bit Amazon Linux 2 v4.9.0 running Tomcat 9 Corretto 11",
        permitted_file_types: &["war", "zip"],
    },
    StackDetail {
        name: "64bit Amazon Linux 2 v2.11.0 running .NET Core",
        permitted_file_types: &["zip"],
    },
    StackDetail {
        name: "64bit Amazon Linux 2 v3.9.0 running Corretto 11",
        permitted_file_types: &["jar", "zip"],
    },
    StackDetail {
        name: "64bit Amazon Linux 2 v3.9.0 running Corretto 8",
        permitted_file_types: &["jar", "zip"],
    },
    StackDetail {
        name: "64bit Amazon Linux 2 v3.13.0 running Go 1",
        permitted_file_types: &["zip"],
    },
    StackDetail {
        name: "64bit Amazon Linux 2 v5.11.0 running Node.js 18",
        permitted_file_types: &["zip"],
    },
    StackDetail {
        name: "64bit Amazon Linux 2 v3.5.0 running ECS",
        permitted_file_types: &[],
    },
    StackDetail {
        name: "64bit Amazon Linux 2 v3.9.0 running Corretto 17",
        permitted_file_types: &["jar", "zip"],
    },
    StackDetail {
        name: "64bit Amazon Linux 2 v3.10.0 running PHP 8.1",
        permitted_file_types: &["zip"],
    },
    StackDetail {
        name: "64bit Amazon Linux 2 v4.9.0 running Tomcat 9 Corretto 8",
        permitted_file_types: &["war", "zip"],
    },
    StackDetail {
        name: "64bit Amazon Linux 2 v4.2.0 running Docker",
        permitted_file_types: &[],
    },
    StackDetail {
        name: "64bit Amazon Linux 2 v3.3.14 running Python 3.8",
        permitted_file_types: &["zip"],
    },
    StackDetail {
        name: "64bit Amazon Linux 2 v5.0.2 running Node.js 12",
        permitted_file_types: &["zip"],
    },
    StackDetail {
        name: "64bit Amazon Linux 2 v5.0.2 running Node.js 10",
        permitted_file_types: &["zip"],
    },
    StackDetail {
        name: "64bit Amazon Linux 2 v0.1.0 running Node.js 12 (BETA)",
        permitted_file_types: &["zip"],
    },
    StackDetail {
        name: "64bit Amazon Linux 2018.03 v2.6.33 running Packer 1.0.3",
        permitted_file_types: &[],
    },
];

// ---------------------------------------------------------------------------
// Location helpers: environments live inside applications, so most ops first
// resolve (app, env) — matching moto's `_find_environment` (name OR id).
// ---------------------------------------------------------------------------

fn env_arn(env: &Environment, region: &str, account: u64) -> String {
    make_arn(
        region,
        account,
        "environment",
        &format!("{}/{}", env.app_name, env.env_name),
    )
}

/// Location of the environment matching name-or-id, if any.
fn find_env_loc(state: &EbState, name: Option<&str>, id: Option<&str>) -> Option<(String, String)> {
    for (app_name, app) in &state.applications {
        for env in app.environments.values() {
            if name.map(|n| env.env_name == n).unwrap_or(false)
                || id.map(|i| env.id == i).unwrap_or(false)
            {
                return Some((app_name.clone(), env.env_name.clone()));
            }
        }
    }
    None
}

fn find_env_loc_by_params(p: &Params, state: &EbState) -> Option<(String, String)> {
    find_env_loc(
        state,
        p_string(p, "EnvironmentName"),
        p_string(p, "EnvironmentId"),
    )
}

/// Environment mutable ref for an already-resolved location.
fn env_at_mut<'a>(
    state: &'a mut EbState,
    app_name: &str,
    env_name: &str,
) -> Option<&'a mut Environment> {
    state
        .applications
        .get_mut(app_name)?
        .environments
        .get_mut(env_name)
}

fn env_arn_mut<'a>(
    req: &Request,
    state: &'a mut EbState,
    arn: &str,
) -> Option<&'a mut Environment> {
    state
        .applications
        .values_mut()
        .flat_map(|app| app.environments.values_mut())
        .find(|env| env_arn(env, &req.region, req.account) == arn)
}

/// moto's message for a failed environment match, interpolating the
/// attempted name-or-id.
fn no_environment_message(p: &Params) -> String {
    let name = p_string(p, "EnvironmentName").unwrap_or("");
    let id = p_string(p, "EnvironmentId").unwrap_or("");
    format!(
        "No Environment named '{}' found.",
        if name.is_empty() { id } else { name }
    )
}

fn env_key(p: &Params) -> String {
    p_string(p, "EnvironmentName")
        .or_else(|| p_string(p, "EnvironmentId"))
        .unwrap_or("")
        .to_string()
}

// ---------------------------------------------------------------------------
// Operations (one per botocore action; bodies are XML fragments).
// ---------------------------------------------------------------------------

fn op_abort_environment_update() -> OpResult {
    // moto acknowledges the call and returns an empty result.
    Ok(String::new())
}

fn op_apply_environment_managed_action(p: &Params) -> OpResult {
    // moto does not validate the environment; it echoes the action id.
    Ok(format!(
        "{}{}{}{}",
        tag_esc("ActionId", p_string(p, "ActionId").unwrap_or("")),
        tag_esc("ActionDescription", "Mock managed action"),
        tag_esc("ActionType", "Unknown"),
        tag_esc("Status", "Applied"),
    ))
}

fn op_associate_environment_operations_role(state: &mut EbState, p: &Params) -> OpResult {
    let env_name = p_string(p, "EnvironmentName").unwrap_or("").to_string();
    let role = p_string(p, "OperationsRole").unwrap_or("").to_string();
    if let Some(loc) = find_env_loc(state, Some(&env_name), None) {
        if let Some(env) = env_at_mut(state, &loc.0, &loc.1) {
            env.operations_role = Some(role);
        }
    }
    // Silently ignore unknown environments (moto comment: AWS behavior).
    Ok(String::new())
}

fn op_disassociate_environment_operations_role(state: &mut EbState, p: &Params) -> OpResult {
    let env_name = p_string(p, "EnvironmentName").unwrap_or("").to_string();
    if let Some(loc) = find_env_loc(state, Some(&env_name), None) {
        if let Some(env) = env_at_mut(state, &loc.0, &loc.1) {
            env.operations_role = None;
        }
    }
    Ok(String::new())
}

fn op_check_dns_availability(req: &Request, state: &EbState, p: &Params) -> OpResult {
    let prefix = p_string(p, "CNAMEPrefix").unwrap_or("");
    let used_cnames: Vec<String> = state
        .applications
        .values()
        .flat_map(|app| app.environments.values())
        .map(|env| env.cname.clone())
        .collect();
    let fully_qualified = format!("{prefix}.{}.elasticbeanstalk.com", req.region);
    let available = !used_cnames.contains(&fully_qualified);
    Ok(format!(
        "{}{}",
        tag_esc("Available", if available { "true" } else { "false" }),
        tag_esc("FullyQualifiedCNAME", &fully_qualified),
    ))
}

fn op_compose_environments(req: &Request, state: &EbState, p: &Params) -> OpResult {
    let app_name = p_string(p, "ApplicationName").unwrap_or("");
    let mut items = String::new();
    if let Some(app) = state.applications.get(app_name) {
        for env in app.environments.values() {
            items.push_str(&environment_member_xml(env, &req.region, req.account));
        }
    }
    Ok(tag_raw("Environments", &items))
}

fn op_create_application(req: &Request, state: &mut EbState, p: &Params) -> OpResult {
    let name = p_string(p, "ApplicationName").unwrap_or("").to_string();
    if state.applications.contains_key(&name) {
        return Err(invalid(format!("Application {name} already exists.")));
    }
    state.applications.insert(
        name.clone(),
        Application {
            description: String::new(),
            date_created: iso_micro(),
            date_updated: iso_micro(),
            versions: BTreeMap::new(),
            templates: BTreeMap::new(),
            environments: BTreeMap::new(),
            lifecycle: Some(parse_lifecycle_config(p, "ResourceLifecycleConfig")),
        },
    );
    let app = state.applications.get(&name).expect("just inserted");
    Ok(format!(
        "<Application>{}</Application>",
        application_xml(&name, app, &req.region, req.account)
    ))
}

fn op_create_application_version(req: &Request, state: &mut EbState, p: &Params) -> OpResult {
    let app_name = p_string(p, "ApplicationName").unwrap_or("").to_string();
    let label = p_string(p, "VersionLabel").unwrap_or("").to_string();
    let Some(app) = state.applications.get_mut(&app_name) else {
        return Err(invalid(format!("No Application named '{app_name}' found.")));
    };
    let source_bundle = p.get("SourceBundle").and_then(as_map).and_then(|m| {
        let bucket = member_string(m, "S3Bucket")?;
        let key = member_string(m, "S3Key")?;
        Some((bucket.to_string(), key.to_string()))
    });
    let version = AppVersion {
        description: p_string(p, "Description").unwrap_or("").to_string(),
        source_bundle,
        date_created: iso_micro(),
        date_updated: iso_micro(),
        arn: make_arn(
            &req.region,
            req.account,
            "applicationversion",
            &format!("{app_name}/{label}"),
        ),
    };
    app.versions.insert(label.clone(), version);
    let stored = app.versions.get(&label).expect("just inserted");
    Ok(format!(
        "<ApplicationVersion>{}</ApplicationVersion>",
        application_version_xml(&app_name, &label, stored),
    ))
}

fn op_create_configuration_template(state: &mut EbState, p: &Params) -> OpResult {
    let app_name = p_string(p, "ApplicationName").unwrap_or("").to_string();
    let template_name = p_string(p, "TemplateName").unwrap_or("").to_string();
    let Some(app) = state.applications.get_mut(&app_name) else {
        return Err(invalid(format!("No Application named '{app_name}' found.")));
    };
    let template = ConfigTemplate {
        template_name: template_name.clone(),
        solution_stack_name: p_string(p, "SolutionStackName")
            .unwrap_or_default()
            .to_string(),
        description: p_string(p, "Description").unwrap_or("").to_string(),
        date_created: iso_micro(),
        date_updated: iso_micro(),
    };
    app.templates.insert(template_name.clone(), template);
    Ok(config_template_xml(
        &app.templates[&template_name],
        &app_name,
    ))
}

fn op_create_environment(req: &Request, state: &mut EbState, p: &Params) -> OpResult {
    let app_name = p_string(p, "ApplicationName").unwrap_or("").to_string();
    let env_name = p_string(p, "EnvironmentName").unwrap_or("").to_string();
    let Some(app) = state.applications.get_mut(&app_name) else {
        return Err(invalid(format!("No Application named '{app_name}' found.")));
    };
    if app.environments.contains_key(&env_name) {
        return Err(invalid(format!("Environment {env_name} already exists.")));
    }
    // Default CNAME is the environment name (or CNAMEPrefix when given).
    let cname_host = p_string(p, "CNAMEPrefix").unwrap_or(&env_name).to_string();
    app.environments.insert(
        env_name.clone(),
        Environment {
            app_name: app_name.clone(),
            env_name: env_name.clone(),
            id: format!("e-{}", rand_hex(10)),
            solution_stack: p_string(p, "SolutionStackName")
                .unwrap_or_default()
                .to_string(),
            description: p_string(p, "Description").map(str::to_string),
            cname: format!("{cname_host}.{}.elasticbeanstalk.com", req.region),
            status: "Ready".into(),
            version_label: "1".to_string(),
            date_created: iso_micro(),
            date_updated: iso_micro(),
            operations_role: None,
            tags: read_tag_list(p, "Tags"),
        },
    );
    Ok(environment_xml(
        &app.environments[&env_name],
        &req.region,
        req.account,
    ))
}

fn op_create_platform_version(req: &Request, state: &mut EbState, p: &Params) -> OpResult {
    let name = p_string(p, "PlatformName")
        .unwrap_or("custom-platform")
        .to_string();
    let version = p_string(p, "PlatformVersion")
        .unwrap_or("1.0.0")
        .to_string();
    let arn = make_arn(
        &req.region,
        req.account,
        "platform",
        &format!("{name}/{version}"),
    );
    state.platform_versions.insert(arn.clone());
    Ok(format!(
        "<PlatformSummary>{}{}</PlatformSummary><Builder>{}</Builder>",
        tag_esc("PlatformArn", &arn),
        tag_esc("PlatformStatus", "Ready"),
        // moto returns Builder: {"ARN": ""} — an empty ARN element.
        tag_esc("ARN", ""),
    ))
}

fn op_create_storage_location(req: &Request) -> OpResult {
    Ok(tag_esc(
        "S3Bucket",
        &format!("elasticbeanstalk-{}-{}", req.region, req.account),
    ))
}

fn op_delete_application(state: &mut EbState, p: &Params) -> OpResult {
    let name = p_string(p, "ApplicationName").unwrap_or("").to_string();
    state.applications.remove(&name);
    Ok(String::new())
}

fn op_delete_application_version(state: &mut EbState, p: &Params) -> OpResult {
    let app_name = p_string(p, "ApplicationName").unwrap_or("").to_string();
    let label = p_string(p, "VersionLabel").unwrap_or("").to_string();
    if let Some(app) = state.applications.get_mut(&app_name) {
        app.versions.remove(&label);
    }
    Ok(String::new())
}

fn op_delete_configuration_template(state: &mut EbState, p: &Params) -> OpResult {
    let app_name = p_string(p, "ApplicationName").unwrap_or("").to_string();
    require_application(state, &app_name)?;
    let template_name = p_string(p, "TemplateName").unwrap_or("").to_string();
    if let Some(app) = state.applications.get_mut(&app_name) {
        app.templates.remove(&template_name);
    }
    Ok(String::new())
}

fn op_delete_environment_configuration() -> OpResult {
    // moto no-ops regardless of existence (draft configuration delete).
    Ok(String::new())
}

fn op_delete_platform_version(state: &mut EbState, p: &Params) -> OpResult {
    let arn = p_string(p, "PlatformArn").unwrap_or("").to_string();
    state.platform_versions.remove(&arn);
    Ok(String::new())
}

fn op_describe_account_attributes() -> OpResult {
    let mut quotas = String::new();
    for (quota, maximum) in [
        ("ApplicationQuota", 75),
        ("ApplicationVersionQuota", 1000),
        ("EnvironmentQuota", 200),
        ("ConfigurationTemplateQuota", 2000),
        ("CustomPlatformQuota", 50),
    ] {
        quotas.push_str(&format!("<{quota}><Maximum>{maximum}</Maximum></{quota}>"));
    }
    Ok(format!("<ResourceQuotas>{quotas}</ResourceQuotas>"))
}

fn op_describe_application_versions(state: &EbState, p: &Params) -> OpResult {
    let app_filter = p_string(p, "ApplicationName");
    let labels = p_list(p, "VersionLabels");
    let mut items = String::new();
    for (app_name, app) in &state.applications {
        for (label, version) in &app.versions {
            if let Some(f) = app_filter {
                if app_name != f {
                    continue;
                }
            }
            if !labels.is_empty() && !labels.iter().any(|l| l == label) {
                continue;
            }
            items.push_str(&format!(
                "<member>{}</member>",
                application_version_xml(app_name, label, version)
            ));
        }
    }
    Ok(tag_raw("ApplicationVersions", &items))
}

fn op_describe_applications(req: &Request, state: &EbState, p: &Params) -> OpResult {
    let names = p_list(p, "ApplicationNames");
    let mut items = String::new();
    for (name, app) in &state.applications {
        if !names.is_empty() && !names.iter().any(|n| n == name) {
            continue;
        }
        items.push_str(&format!(
            "<member>{}</member>",
            application_xml(name, app, &req.region, req.account)
        ));
    }
    Ok(tag_raw("Applications", &items))
}

fn op_describe_configuration_options(p: &Params) -> OpResult {
    let stack = p_string(p, "SolutionStackName").unwrap_or("");
    Ok(format!(
        "{}{}",
        tag_esc("SolutionStackName", stack),
        tag_raw("Options", "")
    ))
}

fn op_describe_environment_health(p: &Params) -> OpResult {
    // moto answers Ready/Ok/Green without checking the environment exists.
    Ok(format!(
        "{}{}{}{}",
        tag_esc(
            "EnvironmentName",
            p_string(p, "EnvironmentName").unwrap_or("")
        ),
        tag_esc("HealthStatus", "Ok"),
        tag_esc("Status", "Ready"),
        tag_esc("Color", "Green"),
    ))
}

fn op_describe_environment_resources(p: &Params) -> OpResult {
    Ok(format!(
        "<EnvironmentResources>{}{}</EnvironmentResources>",
        tag_esc(
            "EnvironmentName",
            p_string(p, "EnvironmentName").unwrap_or("")
        ),
        [
            "AutoScalingGroups",
            "Instances",
            "LaunchConfigurations",
            "LaunchTemplates",
            "LoadBalancers",
            "Triggers",
            "Queues",
        ]
        .iter()
        .map(|t| tag_raw(t, ""))
        .collect::<String>()
    ))
}

fn op_describe_environments(req: &Request, state: &EbState, p: &Params) -> OpResult {
    let names = p_list(p, "EnvironmentNames");
    let ids = p_list(p, "EnvironmentIds");
    let app_filter = p_string(p, "ApplicationName");
    let mut items = String::new();
    for app in state.applications.values() {
        for env in app.environments.values() {
            if !ids.is_empty() && !ids.contains(&env.id) {
                continue;
            }
            if !names.is_empty() && !names.contains(&env.env_name) {
                continue;
            }
            if let Some(f) = app_filter {
                if env.app_name != f {
                    continue;
                }
            }
            items.push_str(&environment_member_xml(env, &req.region, req.account));
        }
    }
    Ok(tag_raw("Environments", &items))
}

fn op_describe_platform_version(p: &Params) -> OpResult {
    Ok(format!(
        "<PlatformDescription>{}{}{}{}{}{}{}{}</PlatformDescription>",
        tag_esc("PlatformArn", p_string(p, "PlatformArn").unwrap_or("")),
        tag_esc("PlatformStatus", "Ready"),
        tag_esc("PlatformCategory", "Custom"),
        tag_esc("OperatingSystemName", "Amazon Linux 2023"),
        tag_esc("OperatingSystemVersion", "2023"),
        tag_raw("ProgrammingLanguages", ""),
        tag_raw("Frameworks", ""),
        tag_raw("CustomAmiList", ""),
    ))
}

fn op_list_available_solution_stacks() -> OpResult {
    let names = members_xml(
        &STACKS
            .iter()
            .map(|s| s.name.to_string())
            .collect::<Vec<_>>(),
    );
    let mut details = String::new();
    for stack in STACKS {
        details.push_str(&format!(
            "<member>{}{}</member>",
            tag_esc("SolutionStackName", stack.name),
            tag_raw(
                "PermittedFileTypes",
                &members_xml(
                    &stack
                        .permitted_file_types
                        .iter()
                        .map(|t| t.to_string())
                        .collect::<Vec<_>>()
                )
            ),
        ));
    }
    Ok(format!(
        "{}{}",
        tag_raw("SolutionStacks", &names),
        tag_raw("SolutionStackDetails", &details),
    ))
}

fn op_list_tags_for_resource(req: &Request, state: &mut EbState, p: &Params) -> OpResult {
    let arn = p_string(p, "ResourceArn").unwrap_or("").to_string();
    let Some(env) = env_arn_mut(req, state, &arn) else {
        return Err(resource_not_found(format!(
            "Resource not found for ARN '{arn}'."
        )));
    };
    Ok(format!(
        "{}{}",
        tag_esc("ResourceArn", &arn),
        tag_raw("ResourceTags", &tags_xml(&env.tags)),
    ))
}

fn op_rebuild_environment(state: &mut EbState, p: &Params) -> OpResult {
    let Some(loc) = find_env_loc_by_params(p, state) else {
        return Err(invalid(no_environment_message(p)));
    };
    if let Some(env) = env_at_mut(state, &loc.0, &loc.1) {
        env.status = "Launching".into();
        env.date_updated = iso_micro();
    }
    Ok(String::new())
}

fn op_request_environment_info(state: &mut EbState, p: &Params) -> OpResult {
    let key = env_key(p);
    let info_type = p_string(p, "InfoType").unwrap_or("tail").to_string();
    let message = format!("Mock {info_type} log for {key}");
    state.environment_info.insert(
        key.clone(),
        vec![EnvInfoItem {
            info_type,
            ec2_instance_id: "i-mock12345".into(),
            sample_timestamp: iso_micro(),
            message,
        }],
    );
    Ok(String::new())
}

fn op_retrieve_environment_info(state: &EbState, p: &Params) -> OpResult {
    let key = env_key(p);
    let items = state
        .environment_info
        .get(&key)
        .map(|items| {
            items
                .iter()
                .map(|i| {
                    format!(
                        "<member>{}{}{}{}</member>",
                        tag_esc("InfoType", &i.info_type),
                        tag_esc("Ec2InstanceId", &i.ec2_instance_id),
                        tag_esc("SampleTimestamp", &i.sample_timestamp),
                        tag_esc("Message", &i.message),
                    )
                })
                .collect::<String>()
        })
        .unwrap_or_default();
    Ok(tag_raw("EnvironmentInfo", &items))
}

fn op_swap_environment_cnames(state: &mut EbState, p: &Params) -> OpResult {
    let src_name = p_string(p, "SourceEnvironmentName");
    let src_id = p_string(p, "SourceEnvironmentId");
    let dst_name = p_string(p, "DestinationEnvironmentName");
    let dst_id = p_string(p, "DestinationEnvironmentId");
    let Some((src_app, src_env)) = find_env_loc(state, src_name, src_id) else {
        let name = src_name.or(src_id).unwrap_or("");
        return Err(invalid(format!("No Environment named '{name}' found.")));
    };
    let Some((dst_app, dst_env)) = find_env_loc(state, dst_name, dst_id) else {
        let name = dst_name.or(dst_id).unwrap_or("");
        return Err(invalid(format!("No Environment named '{name}' found.")));
    };
    let cname_of = |state: &mut EbState, app: &str, env: &str| {
        env_at_mut(state, app, env)
            .expect("resolved location must exist")
            .cname
            .clone()
    };
    let src_cname = cname_of(state, &src_app, &src_env);
    let dst_cname = cname_of(state, &dst_app, &dst_env);
    {
        let env = env_at_mut(state, &src_app, &src_env).expect("resolved location must exist");
        env.cname = dst_cname;
    }
    {
        let env = env_at_mut(state, &dst_app, &dst_env).expect("resolved location must exist");
        env.cname = src_cname;
    }
    Ok(String::new())
}

fn require_application(state: &EbState, name: &str) -> Result<(), EbError> {
    if state.applications.contains_key(name) {
        Ok(())
    } else {
        Err(invalid(format!("No Application named '{name}' found.")))
    }
}

fn op_terminate_environment(req: &Request, state: &mut EbState, p: &Params) -> OpResult {
    let Some(loc) = find_env_loc_by_params(p, state) else {
        return Err(invalid(no_environment_message(p)));
    };
    let region = req.region.clone();
    let account = req.account;
    let xml = {
        let env = env_at_mut(state, &loc.0, &loc.1).expect("resolved location must exist");
        env.status = "Terminated".into();
        env.date_updated = iso_micro();
        environment_xml(env, &region, account)
    };
    if let Some(app) = state.applications.get_mut(&loc.0) {
        app.environments.remove(&loc.1);
    }
    Ok(xml)
}

fn op_update_application(req: &Request, state: &mut EbState, p: &Params) -> OpResult {
    let app_name = p_string(p, "ApplicationName").unwrap_or("").to_string();
    let Some(app) = state.applications.get_mut(&app_name) else {
        return Err(invalid(format!("No Application named '{app_name}' found.")));
    };
    if let Some(d) = p_string(p, "Description") {
        app.description = d.to_string();
    }
    // moto returns exactly ApplicationName + Description + ApplicationArn.
    Ok(format!(
        "<Application>{}{}{}</Application>",
        tag_esc("ApplicationName", &app_name),
        tag_esc("Description", &app.description),
        tag_esc(
            "ApplicationArn",
            &make_arn(&req.region, req.account, "application", &app_name)
        ),
    ))
}

fn op_update_application_resource_lifecycle(state: &mut EbState, p: &Params) -> OpResult {
    let app_name = p_string(p, "ApplicationName").unwrap_or("").to_string();
    let Some(app) = state.applications.get_mut(&app_name) else {
        return Err(invalid(format!("No Application named '{app_name}' found.")));
    };
    let cfg = parse_lifecycle_config(p, "ResourceLifecycleConfig");
    let xml = lifecycle_config_xml(&cfg);
    app.lifecycle = Some(cfg);
    Ok(format!(
        "{}{}",
        tag_esc("ApplicationName", &app_name),
        tag_raw("ResourceLifecycleConfig", &xml),
    ))
}

fn op_update_application_version(state: &mut EbState, p: &Params) -> OpResult {
    let app_name = p_string(p, "ApplicationName").unwrap_or("").to_string();
    let label = p_string(p, "VersionLabel").unwrap_or("").to_string();
    let Some(app) = state.applications.get_mut(&app_name) else {
        return Err(invalid(format!("No Application named '{app_name}' found.")));
    };
    let Some(version) = app.versions.get_mut(&label) else {
        return Err(invalid(format!(
            "No Application Version named '{label}' found."
        )));
    };
    if let Some(d) = p_string(p, "Description") {
        version.description = d.to_string();
    }
    version.date_updated = iso_micro();
    Ok(format!(
        "<ApplicationVersion>{}{}{}{}</ApplicationVersion>",
        tag_esc("ApplicationName", &app_name),
        tag_esc("VersionLabel", &label),
        tag_esc("Description", &version.description),
        tag_esc("Status", "UNPROCESSED"),
    ))
}

fn op_update_configuration_template(state: &mut EbState, p: &Params) -> OpResult {
    let app_name = p_string(p, "ApplicationName").unwrap_or("").to_string();
    let template_name = p_string(p, "TemplateName").unwrap_or("").to_string();
    let Some(app) = state.applications.get_mut(&app_name) else {
        return Err(invalid(format!("No Application named '{app_name}' found.")));
    };
    let Some(template) = app.templates.get_mut(&template_name) else {
        return Err(invalid(format!(
            "No Configuration Template named '{template_name}' found."
        )));
    };
    if let Some(d) = p_string(p, "Description") {
        // moto updates the description; OptionSettings are acknowledged but
        // never surfaced by its response stubs, so they are not stored.
        template.description = d.to_string();
    }
    template.date_updated = iso_micro();
    Ok(config_template_xml(template, &app_name))
}

fn op_update_environment(req: &Request, state: &mut EbState, p: &Params) -> OpResult {
    let Some(loc) = find_env_loc_by_params(p, state) else {
        return Err(invalid(no_environment_message(p)));
    };
    let region = req.region.clone();
    let account = req.account;
    let xml = {
        let env = env_at_mut(state, &loc.0, &loc.1).expect("resolved location must exist");
        env.date_updated = iso_micro();
        if let Some(v) = p_string(p, "VersionLabel") {
            env.version_label = v.to_string();
        }
        if let Some(d) = p_string(p, "Description") {
            // moto's thin rewrite drops the description; storing it keeps
            // real-AWS round-trips (the suite only checks name/id echoes).
            env.description = Some(d.to_string());
        }
        environment_xml(env, &region, account)
    };
    Ok(xml)
}

fn op_update_tags_for_resource(req: &Request, state: &mut EbState, p: &Params) -> OpResult {
    let arn = p_string(p, "ResourceArn").unwrap_or("").to_string();
    let to_add = read_tag_list(p, "TagsToAdd");
    let to_remove = p_list(p, "TagsToRemove");
    let Some(env) = env_arn_mut(req, state, &arn) else {
        return Err(resource_not_found(format!(
            "Resource not found for ARN '{arn}'."
        )));
    };
    for (key, value) in to_add {
        match env.tags.iter_mut().find(|(k, _)| *k == key) {
            Some(slot) => slot.1 = value,
            None => env.tags.push((key, value)),
        }
    }
    for key in &to_remove {
        env.tags.retain(|(k, _)| k != key);
    }
    Ok(String::new())
}

fn op_validate_configuration_settings(state: &EbState, p: &Params) -> OpResult {
    let app_name = p_string(p, "ApplicationName").unwrap_or("");
    if !state.applications.contains_key(app_name) {
        return Err(invalid(format!("No Application named '{app_name}' found.")));
    }
    Ok(tag_raw("Messages", ""))
}

// ---------------------------------------------------------------------------
// Dispatch.
// ---------------------------------------------------------------------------

/// Resolve the action from body/query (the gateway normally sets `operation`).
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

fn result_xml(op: &str, body: &str) -> String {
    let result = if body.is_empty() {
        format!("<{op}Result/>")
    } else {
        format!("<{op}Result>{body}</{op}Result>")
    };
    format!(
        "<{op}Response xmlns=\"{XMLNS}\">{result}<ResponseMetadata><RequestId>{rid}</RequestId></ResponseMetadata></{op}Response>",
        rid = uuid::Uuid::new_v4(),
    )
}

fn err_response(status: u16, code: &str, message: &str) -> Response {
    Response::xml(
        status,
        format!(
            "<ErrorResponse xmlns=\"{XMLNS}\"><Error><Type>Sender</Type><Code>{}</Code><Message>{}</Message></Error><RequestId>{}</RequestId></ErrorResponse>",
            esc(code),
            esc(message),
            uuid::Uuid::new_v4(),
        )
        .into_bytes(),
    )
}

/// Route an EB query-protocol request to the native backend. `None` when the
/// request carries no action (the gateway then answers with a generic 501).
pub fn dispatch(req: &Request) -> Option<Response> {
    let op = action_of(req)?;
    let p = parse_params(req);
    let slots = store::slot::<EbState>("elasticbeanstalk");
    let mut map = slots.write();
    let state = map.entry((req.account, req.region.clone())).or_default();
    match run_op(req, state, &op, &p) {
        Ok(body) => Some(Response::xml(200, result_xml(&op, &body).into_bytes())),
        Err((status, code, message)) => Some(err_response(status, &code, &message)),
    }
}

#[allow(clippy::too_many_lines)]
fn run_op(req: &Request, state: &mut EbState, op: &str, p: &Params) -> OpResult {
    match op {
        "AbortEnvironmentUpdate" => op_abort_environment_update(),
        "ApplyEnvironmentManagedAction" => op_apply_environment_managed_action(p),
        "AssociateEnvironmentOperationsRole" => op_associate_environment_operations_role(state, p),
        "CheckDNSAvailability" => op_check_dns_availability(req, state, p),
        "ComposeEnvironments" => op_compose_environments(req, state, p),
        "CreateApplication" => op_create_application(req, state, p),
        "CreateApplicationVersion" => op_create_application_version(req, state, p),
        "CreateConfigurationTemplate" => op_create_configuration_template(state, p),
        "CreateEnvironment" => op_create_environment(req, state, p),
        "CreatePlatformVersion" => op_create_platform_version(req, state, p),
        "CreateStorageLocation" => op_create_storage_location(req),
        "DeleteApplication" => op_delete_application(state, p),
        "DeleteApplicationVersion" => op_delete_application_version(state, p),
        "DeleteConfigurationTemplate" => op_delete_configuration_template(state, p),
        "DeleteEnvironmentConfiguration" => op_delete_environment_configuration(),
        "DeletePlatformVersion" => op_delete_platform_version(state, p),
        "DescribeAccountAttributes" => op_describe_account_attributes(),
        "DescribeApplicationVersions" => op_describe_application_versions(state, p),
        "DescribeApplications" => op_describe_applications(req, state, p),
        "DescribeConfigurationOptions" => op_describe_configuration_options(p),
        "DescribeConfigurationSettings" => Ok(tag_raw("ConfigurationSettings", "")),
        "DescribeEnvironmentHealth" => op_describe_environment_health(p),
        "DescribeEnvironmentManagedActionHistory" => Ok(tag_raw("ManagedActionHistoryItems", "")),
        "DescribeEnvironmentManagedActions" => Ok(tag_raw("ManagedActions", "")),
        "DescribeEnvironmentResources" => op_describe_environment_resources(p),
        "DescribeEnvironments" => op_describe_environments(req, state, p),
        "DescribeEvents" => Ok(tag_raw("Events", "")),
        "DescribeInstancesHealth" => Ok(tag_raw("InstanceHealthList", "")),
        "DescribePlatformVersion" => op_describe_platform_version(p),
        "DisassociateEnvironmentOperationsRole" => {
            op_disassociate_environment_operations_role(state, p)
        }
        "ListAvailableSolutionStacks" => op_list_available_solution_stacks(),
        "ListPlatformBranches" => Ok(tag_raw("PlatformBranchSummaryList", "")),
        "ListPlatformVersions" => Ok(tag_raw("PlatformSummaryList", "")),
        "ListTagsForResource" => op_list_tags_for_resource(req, state, p),
        "RebuildEnvironment" => op_rebuild_environment(state, p),
        "RequestEnvironmentInfo" => op_request_environment_info(state, p),
        "RestartAppServer" => Ok(String::new()),
        "RetrieveEnvironmentInfo" => op_retrieve_environment_info(state, p),
        "SwapEnvironmentCNAMEs" => op_swap_environment_cnames(state, p),
        "TerminateEnvironment" => op_terminate_environment(req, state, p),
        "UpdateApplication" => op_update_application(req, state, p),
        "UpdateApplicationResourceLifecycle" => op_update_application_resource_lifecycle(state, p),
        "UpdateApplicationVersion" => op_update_application_version(state, p),
        "UpdateConfigurationTemplate" => op_update_configuration_template(state, p),
        "UpdateEnvironment" => op_update_environment(req, state, p),
        "UpdateTagsForResource" => op_update_tags_for_resource(req, state, p),
        "ValidateConfigurationSettings" => op_validate_configuration_settings(state, p),
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
            service: "elasticbeanstalk".into(),
            operation: op.into(),
            method: "POST".into(),
            path: "/".into(),
            query_string: String::new(),
            headers: Default::default(),
            body: bytes::Bytes::from(format!("Action={op}&Version=2010-12-01&{rest}")),
            account: 123456789012,
            region: "us-east-1".into(),
        }
    }

    /// Apply `req` to a persistent per-test state; returns either the body XML
    /// or the (status, code, message) error.
    fn run_again(acct: u64, req: &Request) -> Result<String, EbError> {
        let slots = store::slot::<EbState>("elasticbeanstalk");
        let mut map = slots.write();
        let state = map.entry((acct, "us-east-1".into())).or_default();
        let p = parse_params(req);
        run_op(req, state, &req.operation, &p)
    }

    fn err_code(err: EbError) -> String {
        err.1
    }

    #[test]
    fn application_lifecycle_and_filters() {
        let req = req_for("CreateApplication", "ApplicationName=myapp");
        let body = run_again(1, &req).expect("create");
        assert!(body.contains("<ApplicationName>myapp</ApplicationName>"));
        assert!(body.contains(
            "<ApplicationArn>arn:aws:elasticbeanstalk:us-east-1:123456789012:application/myapp</ApplicationArn>"
        ));
        // duplicate name is InvalidParameterValue
        let err = err_code(run_again(1, &req).unwrap_err());
        assert_eq!(err, "InvalidParameterValue");
        // filtered describe matches only this app
        let req = req_for("DescribeApplications", "ApplicationNames.member.1=myapp");
        let body = run_again(1, &req).expect("describe");
        assert!(body.contains("application/myapp"));
        // a nonmatching filter answers an empty list
        let req = req_for("DescribeApplications", "ApplicationNames.member.1=ghost");
        let body = run_again(1, &req).expect("describe");
        assert_eq!(body, "<Applications/>");
    }

    #[test]
    fn version_crud_and_update_errors() {
        run_again(2, &req_for("CreateApplication", "ApplicationName=myapp")).expect("create app");
        let req = req_for(
            "CreateApplicationVersion",
            "ApplicationName=myapp&VersionLabel=v1&Description=first",
        );
        let body = run_again(2, &req).expect("create version");
        assert!(body.contains("<VersionLabel>v1</VersionLabel>"));
        assert!(body.contains("<ApplicationName>myapp</ApplicationName>"));
        assert!(body.contains("applicationversion/myapp/v1"));
        // filtered describe returns the one member with its description
        let req = req_for(
            "DescribeApplicationVersions",
            "ApplicationName=myapp&VersionLabels.member.1=v1",
        );
        let body = run_again(2, &req).expect("describe");
        assert!(body.contains("<member>"));
        assert!(body.contains("<Description>first</Description>"));
        let req = req_for(
            "DescribeApplicationVersions",
            "ApplicationName=myapp&VersionLabels.member.1=ghost",
        );
        let body = run_again(2, &req).expect("describe");
        assert_eq!(body, "<ApplicationVersions/>");
        // unknown application / version update is InvalidParameterValue
        let req = req_for("UpdateApplication", "ApplicationName=ghost&Description=x");
        assert_eq!(
            err_code(run_again(2, &req).unwrap_err()),
            "InvalidParameterValue"
        );
        let req = req_for(
            "UpdateApplicationVersion",
            "ApplicationName=myapp&VersionLabel=ghost&Description=x",
        );
        assert_eq!(
            err_code(run_again(2, &req).unwrap_err()),
            "InvalidParameterValue"
        );
        // deleting the version makes it disappear
        let req = req_for(
            "DeleteApplicationVersion",
            "ApplicationName=myapp&VersionLabel=v1",
        );
        run_again(2, &req).expect("delete version");
        let req = req_for("DescribeApplicationVersions", "ApplicationName=myapp");
        let body = run_again(2, &req).expect("describe");
        assert_eq!(body, "<ApplicationVersions/>");
    }

    #[test]
    fn environment_lifecycle_roundtrip() {
        run_again(3, &req_for("CreateApplication", "ApplicationName=envapp")).expect("create app");
        let req = req_for(
            "CreateEnvironment",
            "ApplicationName=envapp&EnvironmentName=env1&SolutionStackName=64bit%20Amazon%20Linux%202023%20v4.6.0%20running%20Python%203.11&Description=d1",
        );
        let body = run_again(3, &req).expect("create env");
        assert!(body.contains("<EnvironmentName>env1</EnvironmentName>"));
        assert!(body.contains("<EnvironmentId>e-"));
        assert!(body.contains("<Status>Ready</Status>"));
        assert!(body.contains("<CNAME>env1.us-east-1.elasticbeanstalk.com</CNAME>"));
        assert!(body.contains("<EnvironmentArn>arn:aws:elasticbeanstalk:us-east-1:123456789012:environment/envapp/env1</EnvironmentArn>"));
        assert!(body.contains("<SolutionStackName>64bit Amazon Linux 2023 v4.6.0 running Python 3.11</SolutionStackName>"));
        assert!(body.contains("<Description>d1</Description>"));
        // duplicate env name is InvalidParameterValue
        assert_eq!(
            err_code(run_again(3, &req).unwrap_err()),
            "InvalidParameterValue"
        );
        // name-filtered describe shows exactly one member
        let req = req_for("DescribeEnvironments", "EnvironmentNames.member.1=env1");
        let body = run_again(3, &req).expect("describe");
        assert_eq!(body.matches("<member>").count(), 1);
        // update honors description
        let req = req_for(
            "UpdateEnvironment",
            "EnvironmentName=env1&Description=updated",
        );
        let body = run_again(3, &req).expect("update");
        assert!(body.contains("<Description>updated</Description>"));
        // terminate returns the Terminated record and removes the env
        let req = req_for("TerminateEnvironment", "EnvironmentName=env1");
        let body = run_again(3, &req).expect("terminate");
        assert!(body.contains("<Status>Terminated</Status>"));
        let req = req_for("DescribeEnvironments", "EnvironmentNames.member.1=env1");
        let body = run_again(3, &req).expect("describe");
        assert_eq!(body, "<Environments/>");
    }

    #[test]
    fn tags_round_trip_by_arn() {
        run_again(4, &req_for("CreateApplication", "ApplicationName=tagapp")).expect("create app");
        run_again(
            4,
            &req_for(
                "CreateEnvironment",
                "ApplicationName=tagapp&EnvironmentName=tenv",
            ),
        )
        .expect("create env");
        // unknown ARN is ResourceNotFoundException
        let req = req_for(
            "ListTagsForResource",
            "ResourceArn=arn:aws:elasticbeanstalk:us-east-1:123456789012:environment/tagapp/ghost",
        );
        assert_eq!(
            err_code(run_again(4, &req).unwrap_err()),
            "ResourceNotFoundException"
        );
        // add two tags, see both; remove one, see it gone
        let req = req_for(
            "UpdateTagsForResource",
            "ResourceArn=arn:aws:elasticbeanstalk:us-east-1:123456789012:environment/tagapp/tenv&TagsToAdd.member.1.Key=k1&TagsToAdd.member.1.Value=v1&TagsToAdd.member.2.Key=k2&TagsToAdd.member.2.Value=v2",
        );
        run_again(4, &req).expect("update tags");
        let req = req_for(
            "ListTagsForResource",
            "ResourceArn=arn:aws:elasticbeanstalk:us-east-1:123456789012:environment/tagapp/tenv",
        );
        let body = run_again(4, &req).expect("list tags");
        assert!(body.contains("<Key>k1</Key><Value>v1</Value>"));
        assert!(body.contains("<Key>k2</Key><Value>v2</Value>"));
        let req = req_for(
            "UpdateTagsForResource",
            "ResourceArn=arn:aws:elasticbeanstalk:us-east-1:123456789012:environment/tagapp/tenv&TagsToRemove.member.1=k1",
        );
        run_again(4, &req).expect("remove tag");
        let req = req_for(
            "ListTagsForResource",
            "ResourceArn=arn:aws:elasticbeanstalk:us-east-1:123456789012:environment/tagapp/tenv",
        );
        let body = run_again(4, &req).expect("list tags");
        assert!(!body.contains("k1"));
        assert!(body.contains("<Key>k2</Key><Value>v2</Value>"));
    }

    #[test]
    fn dns_storage_health_and_validation() {
        run_again(5, &req_for("CreateApplication", "ApplicationName=vapp")).expect("create app");
        let req = req_for("CheckDNSAvailability", "CNAMEPrefix=unused-prefix");
        let body = run_again(5, &req).expect("check dns");
        assert!(body.contains("<Available>true</Available>"));
        assert!(body.contains("<FullyQualifiedCNAME>unused-prefix.us-east-1.elasticbeanstalk.com</FullyQualifiedCNAME>"));
        let body = run_again(5, &req_for("CreateStorageLocation", "")).expect("storage");
        assert!(body.contains("<S3Bucket>elasticbeanstalk-us-east-1-123456789012</S3Bucket>"));
        let req = req_for(
            "DescribeEnvironmentHealth",
            "EnvironmentName=nonexistent-env",
        );
        let body = run_again(5, &req).expect("health");
        assert!(body.contains("<HealthStatus>Ok</HealthStatus>"));
        assert!(body.contains("<Status>Ready</Status>"));
        assert!(body.contains("<Color>Green</Color>"));
        // validate on an unknown application is InvalidParameterValue
        let req = req_for(
            "ValidateConfigurationSettings",
            "ApplicationName=ghost&OptionSettings.member.1.Namespace=Nothing",
        );
        assert_eq!(
            err_code(run_again(5, &req).unwrap_err()),
            "InvalidParameterValue"
        );
        run_again(5, &req_for("CreateApplication", "ApplicationName=realapp")).expect("create app");
        let req = req_for("ValidateConfigurationSettings", "ApplicationName=realapp");
        let body = run_again(5, &req).expect("validate");
        assert_eq!(body, "<Messages/>");
    }

    #[test]
    fn configuration_template_crud() {
        run_again(6, &req_for("CreateApplication", "ApplicationName=tapp")).expect("create app");
        let req = req_for(
            "CreateConfigurationTemplate",
            "ApplicationName=tapp&TemplateName=tpl1&SolutionStackName=Some%20Stack",
        );
        let body = run_again(6, &req).expect("create template");
        assert!(body.contains("<ApplicationName>tapp</ApplicationName>"));
        assert!(body.contains("<TemplateName>tpl1</TemplateName>"));
        assert!(body.contains("<SolutionStackName>Some Stack</SolutionStackName>"));
        assert!(body.contains("<DeploymentStatus>deployed</DeploymentStatus>"));
        let req = req_for(
            "UpdateConfigurationTemplate",
            "ApplicationName=tapp&TemplateName=tpl1&Description=updated",
        );
        let body = run_again(6, &req).expect("update template");
        assert!(body.contains("<Description>updated</Description>"));
        // template-not-found is InvalidParameterValue
        let req = req_for(
            "UpdateConfigurationTemplate",
            "ApplicationName=tapp&TemplateName=ghost",
        );
        assert_eq!(
            err_code(run_again(6, &req).unwrap_err()),
            "InvalidParameterValue"
        );
        let req = req_for(
            "DeleteConfigurationTemplate",
            "ApplicationName=tapp&TemplateName=tpl1",
        );
        run_again(6, &req).expect("delete template");
        let req = req_for(
            "UpdateConfigurationTemplate",
            "ApplicationName=tapp&TemplateName=tpl1",
        );
        assert_eq!(
            err_code(run_again(6, &req).unwrap_err()),
            "InvalidParameterValue"
        );
    }

    #[test]
    fn platform_versions_info_and_swap() {
        let req = req_for(
            "CreatePlatformVersion",
            "PlatformName=cplat&PlatformVersion=3.0.0",
        );
        let body = run_again(7, &req).expect("create platform");
        assert!(body.contains(
            "<PlatformArn>arn:aws:elasticbeanstalk:us-east-1:123456789012:platform/cplat/3.0.0</PlatformArn>"
        ));
        assert!(body.contains("<PlatformStatus>Ready</PlatformStatus>"));
        // deleting the same platformArn is a no-op 200
        run_again(
            7,
            &req_for(
                "DeletePlatformVersion",
                "PlatformArn=arn:aws:elasticbeanstalk:us-east-1:123456789012:platform/cplat/3.0.0",
            ),
        )
        .expect("delete platform");
        // request/retrieve environment info round trip
        run_again(
            7,
            &req_for(
                "RequestEnvironmentInfo",
                "EnvironmentName=tenv&InfoType=tail",
            ),
        )
        .expect("request info");
        let body = run_again(
            7,
            &req_for("RetrieveEnvironmentInfo", "EnvironmentName=tenv"),
        )
        .expect("retrieve info");
        assert!(body.contains("<InfoType>tail</InfoType>"));
        assert!(body.contains("Mock tail log for tenv"));
        // swapping unknown environments is InvalidParameterValue
        let req = req_for(
            "SwapEnvironmentCNAMEs",
            "SourceEnvironmentName=ghost1&DestinationEnvironmentName=ghost2",
        );
        assert_eq!(
            err_code(run_again(7, &req).unwrap_err()),
            "InvalidParameterValue"
        );
    }

    #[test]
    fn stubs_and_tables() {
        let body = run_again(8, &req_for("ListAvailableSolutionStacks", "")).expect("stacks");
        assert!(body
            .contains("<SolutionStackName>64bit Amazon Linux 2023 v4.6.0 running Python 3.11</SolutionStackName>"));
        assert!(body
            .contains("<SolutionStackName>64bit Amazon Linux 2018.03 v2.6.33 running Packer 1.0.3</SolutionStackName>"));
        assert!(body.contains(
            "<PermittedFileTypes><member>jar</member><member>zip</member></PermittedFileTypes>"
        ));
        assert_eq!(
            run_again(8, &req_for("ListPlatformVersions", "")).expect("list platform versions"),
            "<PlatformSummaryList/>"
        );
        assert_eq!(
            run_again(8, &req_for("ListPlatformBranches", "")).expect("list branches"),
            "<PlatformBranchSummaryList/>"
        );
        assert_eq!(
            run_again(
                8,
                &req_for("DescribeConfigurationSettings", "ApplicationName=nada")
            )
            .expect("settings stub"),
            "<ConfigurationSettings/>"
        );
        assert_eq!(
            run_again(8, &req_for("DescribeEvents", "")).expect("events stub"),
            "<Events/>"
        );
        let body = run_again(8, &req_for("DescribePlatformVersion", "PlatformArn=arn:x"))
            .expect("platform version");
        assert!(body.contains("<PlatformDescription><PlatformArn>arn:x</PlatformArn>"));
        assert!(body.contains("<PlatformCategory>Custom</PlatformCategory>"));
        let body = run_again(8, &req_for("DescribeAccountAttributes", "")).expect("quotas");
        assert!(body.contains("<EnvironmentQuota><Maximum>200</Maximum></EnvironmentQuota>"));
        assert!(body.contains(
            "<ConfigurationTemplateQuota><Maximum>2000</Maximum></ConfigurationTemplateQuota>"
        ));
        let body = run_again(
            8,
            &req_for("DescribeEnvironmentResources", "EnvironmentName=nada"),
        )
        .expect("resources stub");
        assert!(body.starts_with("<EnvironmentResources>"));
        assert!(body.contains("<Queues/>"));
        // managed action and managed history stubs
        let req = req_for(
            "ApplyEnvironmentManagedAction",
            "EnvironmentName=whatever&ActionId=act-1",
        );
        let body = run_again(8, &req).expect("managed action");
        assert!(body.contains("<ActionId>act-1</ActionId>"));
        assert!(body.contains("<Status>Applied</Status>"));
        let body = run_again(8, &req_for("DescribeEnvironmentManagedActions", ""))
            .expect("managed actions stub");
        assert_eq!(body, "<ManagedActions/>");
    }

    #[test]
    fn resource_lifecycle_echo_roundtrip() {
        run_again(9, &req_for("CreateApplication", "ApplicationName=lcapp")).expect("create app");
        let req = req_for(
            "UpdateApplicationResourceLifecycle",
            "ApplicationName=lcapp&ResourceLifecycleConfig.VersionLifecycleConfig.MaxCountRule.Enabled=false&ResourceLifecycleConfig.VersionLifecycleConfig.MaxCountRule.MaxCount=5&ResourceLifecycleConfig.VersionLifecycleConfig.MaxCountRule.DeleteSourceFromS3=false",
        );
        let body = run_again(9, &req).expect("update lifecycle");
        assert!(body.contains("<ApplicationName>lcapp</ApplicationName>"));
        assert!(body.contains("<MaxCountRule><Enabled>false</Enabled><MaxCount>5</MaxCount><DeleteSourceFromS3>false</DeleteSourceFromS3></MaxCountRule>"));
    }

    #[test]
    fn rebuild_and_abort_and_unimplemented() {
        run_again(10, &req_for("CreateApplication", "ApplicationName=rapp")).expect("create app");
        run_again(
            10,
            &req_for(
                "CreateEnvironment",
                "ApplicationName=rapp&EnvironmentName=renv",
            ),
        )
        .expect("create env");
        run_again(10, &req_for("RebuildEnvironment", "EnvironmentName=renv")).expect("rebuild");
        run_again(
            10,
            &req_for("AbortEnvironmentUpdate", "EnvironmentName=renv"),
        )
        .expect("abort");
        run_again(10, &req_for("RestartAppServer", "EnvironmentName=renv")).expect("restart");
        // env survives rebuild/abort/restart with Launching status from rebuild
        let body = run_again(
            10,
            &req_for("DescribeEnvironments", "EnvironmentNames.member.1=renv"),
        )
        .expect("describe");
        assert!(body.contains("<Status>Launching</Status>"));
        // unknown actions answer 501
        assert_eq!(
            err_code(run_again(10, &req_for("NotARobotOperation", "")).unwrap_err()),
            "NotImplemented"
        );
    }
}
