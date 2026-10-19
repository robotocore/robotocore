//! AWS Resource Access Manager backend (REST-JSON, one URI per operation).
//!
//! Port of moto's `ram` backend: resource shares, managed permissions,
//! invitations, principal/resource associations, tagging and the RAM
//! permission work queue. Wire shapes mirror moto's lowercase camel JSON
//! keys; every moto error renders as HTTP 400 with `__type` + `message`
//! (moto's `JsonRESTError`), not 404.
//!
//! Requests enter with the ram sigv4 credential scope and an empty
//! `operation` (the gateway keeps no REST op table for ram), so this
//! module resolves the operation from the HTTP method + botocore
//! requestUri (lowercase camel, one path per operation).
//!
//! Known moto surface boundary, kept faithful: `OTHER-ACCOUNTS` as
//! `resourceOwner` raises `NotImplementedError` moto-side; this port
//! answers the wire `NotImplemented` (501) for that value.

use crate::store;
use crate::{Request, Response};
use once_cell::sync::Lazy;
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

struct Share {
    arn: String,
    name: Option<String>,
    allow_external: bool,
    creation_time: f64,
    last_updated: f64,
    status: &'static str,
    principals: Vec<String>,
    resource_arns: Vec<String>,
    tags: Vec<(String, String)>,
    permission_arns: Vec<String>,
}

impl Share {
    /// moto also emits `featureSet: "STANDARD"` here.
    fn describe(&self, account: u64) -> Value {
        json!({
            "allowExternalPrincipals": self.allow_external,
            "featureSet": "STANDARD",
            "creationTime": self.creation_time,
            "lastUpdatedTime": self.last_updated,
            "name": self.name,
            "owningAccountId": account.to_string(),
            "resourceShareArn": self.arn,
            "status": self.status,
        })
    }
}

struct PermissionVersion {
    version: String,
    default_version: bool,
    creation_time: String,
    status: String,
}

struct Permission {
    name: Option<String>,
    arn: String,
    resource_type: String,
    version: String,
    default_version: bool,
    status: String,
    is_resource_type_default: bool,
    permission_type: &'static str,
    creation_time: String,
    last_updated: String,
    policy_template: Option<String>,
    versions: Vec<PermissionVersion>,
    tags: Vec<(String, String)>,
}

impl Permission {
    /// moto's `ManagedPermission.describe()`.
    fn describe(&self) -> Value {
        json!({
            "arn": self.arn,
            "defaultVersion": self.default_version,
            "isResourceTypeDefault": self.is_resource_type_default,
            "lastUpdatedTime": self.last_updated,
            "name": self.name,
            "permissionType": self.permission_type,
            "resourceType": self.resource_type,
            "status": self.status,
            "creationTime": self.creation_time,
            "version": self.version,
        })
    }

    /// moto's `ManagedPermission.describe_detail()`.
    fn describe_detail(&self, version: Option<i64>) -> Value {
        let mut result = self.describe();
        result["permission"] = json!(self.policy_template.clone().unwrap_or_else(|| "{}".into()));
        if let Some(v) = version {
            result["version"] = json!(v.to_string());
        }
        result
    }

    /// moto's `ManagedPermission.create_version()`.
    fn create_version(&mut self, policy_template: &str) -> Value {
        let now = now_str();
        let new_version = (self.versions.len() + 1).to_string();
        self.versions.push(PermissionVersion {
            version: new_version.clone(),
            default_version: false,
            creation_time: now.clone(),
            status: "CREATED".into(),
        });
        json!({
            "arn": self.arn,
            "defaultVersion": false,
            "isResourceTypeDefault": self.is_resource_type_default,
            "lastUpdatedTime": &now,
            "name": self.name,
            "permission": policy_template,
            "permissionType": self.permission_type,
            "resourceType": self.resource_type,
            "status": "CREATED",
            "creationTime": &now,
            "version": new_version,
        })
    }

    /// moto's `ManagedPermission.list_versions()`.
    fn list_versions(&self) -> Vec<Value> {
        self.versions
            .iter()
            .map(|v| {
                json!({
                    "arn": self.arn,
                    "defaultVersion": v.default_version,
                    "isResourceTypeDefault": self.is_resource_type_default,
                    "lastUpdatedTime": v.creation_time,
                    "name": self.name,
                    "permissionType": self.permission_type,
                    "resourceType": self.resource_type,
                    "status": v.status,
                    "creationTime": v.creation_time,
                    "version": v.version,
                })
            })
            .collect()
    }
}

struct Invitation {
    arn: String,
    share_arn: String,
    share_name: String,
    sender: String,
    receiver: String,
    timestamp: f64,
    status: &'static str,
}

impl Invitation {
    /// moto's `ResourceShareInvitation.describe()`.
    fn describe(&self) -> Value {
        json!({
            "invitationTimestamp": self.timestamp,
            "receiverAccountId": self.receiver,
            "resourceShareArn": self.share_arn,
            "resourceShareAssociations": [],
            "resourceShareInvitationArn": self.arn,
            "resourceShareName": self.share_name,
            "senderAccountId": self.sender,
            "status": self.status,
        })
    }
}

/// AWS-managed permission seeds, from moto's `ram/utils.py`:
/// (name, resourceType, version, is-resource-type-default, creation time).
const MANAGED_PROVIDED: &[(&str, &str, &str, bool, &str)] = &[
    (
        "AWSRAMDefaultPermissionRDSCluster",
        "rds:Cluster",
        "1",
        true,
        "2022-06-30 17:04:16.671000",
    ),
    (
        "AWSRAMDefaultPermissionImageBuilderComponent",
        "imagebuilder:Component",
        "1",
        true,
        "2022-06-30 17:04:21.977000",
    ),
    (
        "AWSRAMDefaultPermissionsNetworkManagerCoreNetwork",
        "networkmanager:CoreNetwork",
        "3",
        true,
        "2024-10-23 16:34:51.604000",
    ),
    (
        "AWSRAMTransitGatewayPermissionsNetworkManagerCoreNetwork",
        "networkmanager:CoreNetwork",
        "1",
        false,
        "2022-06-30 17:03:48.071000",
    ),
    (
        "AWSRAMVPCPermissionsNetworkManagerCoreNetwork",
        "networkmanager:CoreNetwork",
        "1",
        false,
        "2022-06-30 17:03:46.477000",
    ),
    (
        "AWSRAMDefaultPermissionResourceGroup",
        "resource-groups:Group",
        "1",
        true,
        "2022-06-30 17:04:10.914000",
    ),
    (
        "AWSRAMDefaultPermissionImageBuilderImage",
        "imagebuilder:Image",
        "1",
        true,
        "2022-06-30 17:04:21.201000",
    ),
    (
        "AWSRAMDefaultPermissionImageBuilderImageRecipe",
        "imagebuilder:ImageRecipe",
        "1",
        true,
        "2022-06-30 17:04:22.832000",
    ),
    (
        "AWSRAMDefaultPermissionLicenseConfiguration",
        "license-manager:LicenseConfiguration",
        "1",
        true,
        "2024-01-31 16:36:13.664000",
    ),
    (
        "AWSRAMDefaultPermissionAppMesh",
        "appmesh:Mesh",
        "3",
        true,
        "2023-03-28 12:27:55.910000",
    ),
    (
        "AWSRAMDefaultPermissionPrefixList",
        "ec2:PrefixList",
        "1",
        true,
        "2022-06-30 17:04:20.594000",
    ),
    (
        "AWSRAMDefaultPermissionCodeBuildProject",
        "codebuild:Project",
        "1",
        true,
        "2022-06-30 17:04:17.595000",
    ),
    (
        "AWSRAMDefaultPermissionCodeBuildReportGroup",
        "codebuild:ReportGroup",
        "1",
        true,
        "2022-06-30 17:04:17.761000",
    ),
    (
        "AWSRAMDefaultPermissionResolverRule",
        "route53resolver:ResolverRule",
        "1",
        true,
        "2022-06-30 17:04:14.752000",
    ),
    (
        "AWSRAMDefaultPermissionSubnet",
        "ec2:Subnet",
        "1",
        true,
        "2024-10-30 15:11:41.358000",
    ),
    (
        "AWSRAMDefaultPermissionTransitGatewayMulticastDomain",
        "ec2:TransitGatewayMulticastDomain",
        "1",
        true,
        "2022-06-30 17:04:13.461000",
    ),
    (
        "AWSRAMDefaultPermissionGlueDatabase",
        "glue:Database",
        "1",
        true,
        "2022-06-30 17:04:25.912000",
    ),
    (
        "AWSRAMLFEnabledGlueAllTablesReadWriteForDatabase",
        "glue:Database",
        "1",
        false,
        "2023-06-27 17:05:24.471000",
    ),
    (
        "AWSRAMLFEnabledGlueDatabaseReadWrite",
        "glue:Database",
        "1",
        false,
        "2023-06-27 17:05:27.153000",
    ),
    (
        "AWSRAMPermissionGlueAllTablesReadWriteForDatabase",
        "glue:Database",
        "3",
        false,
        "2022-10-27 14:19:43.345000",
    ),
    (
        "AWSRAMPermissionGlueDatabaseReadWrite",
        "glue:Database",
        "3",
        false,
        "2022-10-27 14:20:08.415000",
    ),
    (
        "AWSRAMPermissionGlueTableReadWriteForDatabase",
        "glue:Database",
        "3",
        false,
        "2022-10-27 14:19:17.062000",
    ),
    (
        "AWSRAMPermissionLFTagGlueDatabaseReadWrite",
        "glue:Database",
        "1",
        false,
        "2022-10-27 14:22:14.138000",
    ),
    (
        "AWSRAMPermissionLFTagGlueTableReadWriteForDatabase",
        "glue:Database",
        "1",
        false,
        "2022-10-27 14:21:49.920000",
    ),
    (
        "AWSRAMDefaultPermissionGlueTable",
        "glue:Table",
        "1",
        true,
        "2022-06-30 17:04:24.372000",
    ),
    (
        "AWSRAMLFEnabledGlueDatabaseReadWriteForTable",
        "glue:Table",
        "1",
        false,
        "2023-06-27 17:05:33.534000",
    ),
    (
        "AWSRAMLFEnabledGlueTableReadWrite",
        "glue:Table",
        "1",
        false,
        "2023-06-27 17:05:21.095000",
    ),
    (
        "AWSRAMPermissionGlueDatabaseReadWriteForTable",
        "glue:Table",
        "2",
        false,
        "2022-06-30 17:04:09.034000",
    ),
    (
        "AWSRAMPermissionGlueTableReadWrite",
        "glue:Table",
        "2",
        false,
        "2022-06-30 17:04:03.947000",
    ),
    (
        "AWSRAMPermissionLFTagGlueDatabaseReadWriteForTable",
        "glue:Table",
        "1",
        false,
        "2022-10-27 14:22:26.191000",
    ),
    (
        "AWSRAMPermissionLFTagGlueTableReadWrite",
        "glue:Table",
        "1",
        false,
        "2022-10-27 14:22:01.908000",
    ),
    (
        "AWSRAMDefaultPermissionGlueCatalog",
        "glue:Catalog",
        "1",
        true,
        "2022-06-30 17:04:26.612000",
    ),
    (
        "AWSRAMPermissionGlueAllTablesReadWriteForCatalog",
        "glue:Catalog",
        "3",
        false,
        "2022-10-27 14:19:56.250000",
    ),
    (
        "AWSRAMPermissionGlueDatabaseReadWriteForCatalog",
        "glue:Catalog",
        "3",
        false,
        "2022-10-27 14:20:20.322000",
    ),
    (
        "AWSRAMPermissionGlueTableReadWriteForCatalog",
        "glue:Catalog",
        "3",
        false,
        "2022-10-27 14:19:30.644000",
    ),
    (
        "AWSRAMPermissionLFTagGlueDatabaseReadWriteForCatalog",
        "glue:Catalog",
        "1",
        false,
        "2022-10-27 14:22:37.435000",
    ),
    (
        "AWSRAMPermissionLFTagGlueTableReadWriteForCatalog",
        "glue:Catalog",
        "1",
        false,
        "2022-10-27 14:21:37.593000",
    ),
];

/// Shareable RAM resource types, from moto's `ram/utils.py`: (resourceType,
/// serviceName, resourceRegionScope).
const PROVIDED_RESOURCE_TYPES: &[(&str, &str, &str)] = &[
    ("rds:Cluster", "rds", "REGIONAL"),
    ("imagebuilder:Component", "imagebuilder", "REGIONAL"),
    ("networkmanager:CoreNetwork", "networkmanager", "GLOBAL"),
    ("resource-groups:Group", "resource-groups", "REGIONAL"),
    ("imagebuilder:Image", "imagebuilder", "REGIONAL"),
    ("imagebuilder:ImageRecipe", "imagebuilder", "REGIONAL"),
    (
        "license-manager:LicenseConfiguration",
        "license-manager",
        "REGIONAL",
    ),
    ("appmesh:Mesh", "appmesh", "REGIONAL"),
    ("ec2:PrefixList", "ec2", "REGIONAL"),
    ("codebuild:Project", "codebuild", "REGIONAL"),
    ("codebuild:ReportGroup", "codebuild", "REGIONAL"),
    (
        "route53resolver:ResolverRule",
        "route53resolver",
        "REGIONAL",
    ),
    ("ec2:Subnet", "ec2", "REGIONAL"),
    ("ec2:TransitGatewayMulticastDomain", "ec2", "REGIONAL"),
    ("glue:Database", "glue", "REGIONAL"),
    ("glue:Table", "glue", "REGIONAL"),
    ("glue:Catalog", "glue", "REGIONAL"),
];

/// Resource kinds moto allows inside a shared resource ARN (moto's
/// `ResourceShare.SHAREABLE_RESOURCES`).
const SHAREABLE_RESOURCES: &[&str] = &[
    "cluster",
    "component",
    "core-network",
    "group",
    "image",
    "image-recipe",
    "license-configuration",
    "mesh",
    "prefix-list",
    "project",
    "report-group",
    "resolver-rule",
    "subnet",
    "transit-gateway",
    "database",
    "table",
    "catalog",
];

#[derive(Default)]
struct State {
    shares: Vec<Share>,
    invitations: Vec<Invitation>,
    replace_work_items: Vec<Value>,
    managed_permissions: Vec<Permission>,
}

fn state() -> store::Slot<State> {
    static SLOT: Lazy<store::Slot<State>> = Lazy::new(|| store::slot("ram"));
    SLOT.clone()
}

fn seed_permissions() -> Vec<Permission> {
    MANAGED_PROVIDED
        .iter()
        .map(|(name, resource_type, version, is_default, created)| {
            let mut p = Permission {
                name: Some((*name).into()),
                arn: format!("arn:aws:ram::aws:permission/{name}"),
                resource_type: (*resource_type).into(),
                version: (*version).into(),
                default_version: *is_default,
                status: "ATTACHABLE".into(),
                is_resource_type_default: *is_default,
                permission_type: "AWS_MANAGED",
                creation_time: (*created).into(),
                last_updated: (*created).into(),
                policy_template: None,
                versions: Vec::new(),
                tags: Vec::new(),
            };
            p.versions.push(PermissionVersion {
                version: (*version).into(),
                default_version: *is_default,
                creation_time: (*created).into(),
                status: "ATTACHABLE".into(),
            });
            p
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Wire helpers
// ---------------------------------------------------------------------------

fn ok(v: &Value) -> Response {
    Response::json(200, serde_json::to_vec(v).unwrap_or_default())
}

fn error(status: u16, code: &str, message: &str) -> Response {
    let body = json!({"__type": code, "message": message});
    let mut resp = Response::json(status, serde_json::to_vec(&body).unwrap_or_default());
    resp.headers
        .push(("x-amzn-ErrorType".to_string(), format!("{code}:")));
    resp
}

fn invalid_parameter(message: impl AsRef<str>) -> Response {
    error(400, "InvalidParameterException", message.as_ref())
}

fn malformed_arn(message: impl AsRef<str>) -> Response {
    error(400, "MalformedArnException", message.as_ref())
}

fn unknown_resource(message: impl AsRef<str>) -> Response {
    error(400, "UnknownResourceException", message.as_ref())
}

fn not_implemented(message: impl AsRef<str>) -> Response {
    error(501, "NotImplemented", message.as_ref())
}

fn conflict(message: impl AsRef<str>) -> Response {
    error(400, "OperationNotPermittedException", message.as_ref())
}

/// moto's managed-permission clock format: "YYYY-MM-DD HH:MM:SS.mmm".
fn now_str() -> String {
    chrono::Utc::now()
        .format("%Y-%m-%d %H:%M:%S.%3f")
        .to_string()
}

/// moto's `unix_time`: float epoch seconds.
fn now_unix() -> f64 {
    crate::proto::now_unix()
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

fn param_str<'a>(p: &'a Value, key: &str) -> Option<&'a str> {
    p.get(key).and_then(Value::as_str)
}

fn param_bool(p: &Value, key: &str, default: bool) -> bool {
    p.get(key).and_then(Value::as_bool).unwrap_or(default)
}

fn param_list(p: &Value, key: &str) -> Vec<String> {
    p.get(key)
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .map(|v| v.as_str().unwrap_or_default().to_string())
                .collect()
        })
        .unwrap_or_default()
}

fn param_i64(p: &Value, key: &str) -> Option<i64> {
    match p.get(key) {
        Some(Value::Number(n)) => n.as_i64(),
        Some(Value::String(s)) => s.trim().parse::<i64>().ok(),
        _ => None,
    }
}

/// Tag list from the wire: `[{"key": .., "value": ..}]`.
fn param_tags(p: &Value, key: &str) -> Vec<(String, String)> {
    p.get(key)
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .map(|t| {
                    (
                        t.get("key")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_string(),
                        t.get("value")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_string(),
                    )
                })
                .collect()
        })
        .unwrap_or_default()
}

fn is_12_digits(s: &str) -> bool {
    s.len() == 12 && s.chars().all(|c| c.is_ascii_digit())
}

// ---------------------------------------------------------------------------
// Principal / resource ARN validation (moto's `add_principals` /
// `add_resources`, translated without a regex dependency)
// ---------------------------------------------------------------------------

/// `arn:aws:organizations::<12 digits>:organization/(o-\w+)` → the o-id.
fn parse_org_arn(p: &str) -> Option<String> {
    let rest = p.strip_prefix("arn:aws:organizations::")?;
    let (acct, tail) = rest.split_once(':')?;
    if !is_12_digits(acct) {
        return None;
    }
    let id = tail.strip_prefix("organization/o-")?;
    if id.is_empty() || !id.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return None;
    }
    Some(format!("o-{id}"))
}

/// `arn:aws:organizations::<12 digits>:ou/(o-\w+)/(ou-[\w-]+)` → (o-id, ou-id).
fn parse_ou_arn(p: &str) -> Option<(String, String)> {
    let rest = p.strip_prefix("arn:aws:organizations::")?;
    let (acct, tail) = rest.split_once(':')?;
    if !is_12_digits(acct) {
        return None;
    }
    let tail = tail.strip_prefix("ou/")?;
    let (oid, ouid) = tail.split_once('/')?;
    let oid = oid.strip_prefix("o-")?;
    let ouid = ouid.strip_prefix("ou-")?;
    if oid.is_empty()
        || ouid.is_empty()
        || !oid.chars().all(|c| c.is_alphanumeric() || c == '_')
        || !ouid
            .chars()
            .all(|c| c.is_alphanumeric() || c == '_' || c == '-')
    {
        return None;
    }
    Some((format!("o-{oid}"), format!("ou-{ouid}")))
}

/// moto's principal regex for IAM root/role/user ARNs. `partitioned` is the
/// remainder after `arn:aws` (e.g. `-cn:iam::…`, `:iam::…`).
fn is_iam_principal(p: &str) -> bool {
    let Some(rest) = p.strip_prefix("arn:aws") else {
        return false;
    };
    // Split the partition suffix: "-cn", "-us-gov", or none.
    let rest = if let Some(r) = rest.strip_prefix("-cn") {
        r
    } else if let Some(r) = rest.strip_prefix("-us-gov") {
        r
    } else {
        rest
    };
    let Some(rest) = rest.strip_prefix(":iam::") else {
        return false;
    };
    let (acct, tail) = match rest.split_once(':') {
        Some(kv) => kv,
        None => return false,
    };
    if !is_12_digits(acct) {
        return false;
    }
    tail == "root" || tail.starts_with("role/") || tail.starts_with("user/")
}

fn is_service_principal(p: &str) -> bool {
    match p.split_once('.') {
        Some((head, _)) => {
            !head.is_empty()
                && head
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
                && p.ends_with(".amazonaws.com")
        }
        None => false,
    }
}

/// moto's resource-ARN regex: `arn:aws:<service>:[region]:<12 digits>:
/// <([a-z-]+)/composed>` → the `[a-z-]+` resource kind, or None when the
/// shape does not match at all.
fn parse_resource_arn(arn: &str) -> Option<String> {
    let rest = arn.strip_prefix("arn:aws:")?;
    let (svc, rest) = rest.split_once(':')?;
    if svc.is_empty()
        || !svc
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
    {
        return None;
    }
    let (region, rest) = rest.split_once(':')?;
    if !region
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
    {
        return None;
    }
    let (account, rest) = rest.split_once(':')?;
    if !is_12_digits(account) {
        return None;
    }
    // resource: `([a-z-]+)[/:]...`
    let end = rest.find(['/', ':'])?;
    let kind = &rest[..end];
    if kind.is_empty() || !kind.chars().all(|c| c.is_ascii_lowercase() || c == '-') {
        return None;
    }
    Some(kind.to_string())
}

fn validate_principals(
    org: &Option<(String, Vec<String>)>,
    principals: &[String],
) -> Result<(), Response> {
    for principal in principals {
        if let Some(org_id) = parse_org_arn(principal) {
            match org {
                Some((existing, _)) if existing == &org_id => continue,
                _ => {
                    return Err(unknown_resource(format!(
                        "Organization {org_id} could not be found."
                    )));
                }
            }
        }
        if let Some((org_id, ou_id)) = parse_ou_arn(principal) {
            let known = org
                .as_ref()
                .filter(|(existing, ou_arns)| {
                    existing == &org_id && ou_arns.iter().any(|arn| arn == principal)
                })
                .is_some();
            if !known {
                return Err(unknown_resource(format!(
                    "OrganizationalUnit {ou_id} in unknown organization could not be found."
                )));
            }
            continue;
        }
        if !is_12_digits(principal)
            && !is_iam_principal(principal)
            && !is_service_principal(principal)
        {
            return Err(invalid_parameter(format!(
                "Principal ID {principal} is malformed. Verify the ID and try again."
            )));
        }
    }
    Ok(())
}

fn validate_resource_arns(resource_arns: &[String]) -> Result<(), Response> {
    for resource in resource_arns {
        let Some(kind) = parse_resource_arn(resource) else {
            return Err(malformed_arn(format!(
                "The specified resource ARN {resource} is not valid. Verify the ARN and try again."
            )));
        };
        if !SHAREABLE_RESOURCES.contains(&kind.as_str()) {
            return Err(malformed_arn(
                "You cannot share the selected resource type.",
            ));
        }
    }
    Ok(())
}

/// The service-code part of a resource ARN, lowercased and stripped after
/// the first `:`/`/` (moto's list_resources / list_pending_invitation_resources).
fn resource_type_of(arn: &str) -> String {
    parse_resource_arn(arn).unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Operation resolution: (method, requestUri) -> operation, per botocore's
// ram-2018-01-04 model.
// ---------------------------------------------------------------------------

const RULES: &[(&str, &str, &str)] = &[
    (
        "POST",
        "acceptresourceshareinvitation",
        "AcceptResourceShareInvitation",
    ),
    ("POST", "associateresourceshare", "AssociateResourceShare"),
    (
        "POST",
        "associateresourcesharepermission",
        "AssociateResourceSharePermission",
    ),
    ("POST", "createpermission", "CreatePermission"),
    ("POST", "createpermissionversion", "CreatePermissionVersion"),
    ("POST", "createresourceshare", "CreateResourceShare"),
    ("DELETE", "deletepermission", "DeletePermission"),
    (
        "DELETE",
        "deletepermissionversion",
        "DeletePermissionVersion",
    ),
    ("DELETE", "deleteresourceshare", "DeleteResourceShare"),
    (
        "POST",
        "disassociateresourceshare",
        "DisassociateResourceShare",
    ),
    (
        "POST",
        "disassociateresourcesharepermission",
        "DisassociateResourceSharePermission",
    ),
    (
        "POST",
        "enablesharingwithawsorganization",
        "EnableSharingWithAwsOrganization",
    ),
    ("POST", "getpermission", "GetPermission"),
    ("POST", "getresourcepolicies", "GetResourcePolicies"),
    (
        "POST",
        "getresourceshareassociations",
        "GetResourceShareAssociations",
    ),
    (
        "POST",
        "getresourceshareinvitations",
        "GetResourceShareInvitations",
    ),
    ("POST", "getresourceshares", "GetResourceShares"),
    (
        "POST",
        "listpendinginvitationresources",
        "ListPendingInvitationResources",
    ),
    (
        "POST",
        "listpermissionassociations",
        "ListPermissionAssociations",
    ),
    ("POST", "listpermissionversions", "ListPermissionVersions"),
    ("POST", "listpermissions", "ListPermissions"),
    ("POST", "listprincipals", "ListPrincipals"),
    (
        "POST",
        "listreplacepermissionassociationswork",
        "ListReplacePermissionAssociationsWork",
    ),
    (
        "POST",
        "listresourcesharepermissions",
        "ListResourceSharePermissions",
    ),
    ("POST", "listresources", "ListResources"),
    ("POST", "listresourcetypes", "ListResourceTypes"),
    ("POST", "listsourceassociations", "ListSourceAssociations"),
    (
        "POST",
        "promotepermissioncreatedfrompolicy",
        "PromotePermissionCreatedFromPolicy",
    ),
    (
        "POST",
        "promoteresourcesharecreatedfrompolicy",
        "PromoteResourceShareCreatedFromPolicy",
    ),
    (
        "POST",
        "rejectresourceshareinvitation",
        "RejectResourceShareInvitation",
    ),
    (
        "POST",
        "replacepermissionassociations",
        "ReplacePermissionAssociations",
    ),
    (
        "POST",
        "setdefaultpermissionversion",
        "SetDefaultPermissionVersion",
    ),
    ("POST", "tagresource", "TagResource"),
    ("POST", "untagresource", "UntagResource"),
    ("POST", "updateresourceshare", "UpdateResourceShare"),
];

fn wire_operation(req: &Request) -> Option<&'static str> {
    let path = req.path.trim_start_matches('/').trim_end_matches('/');
    RULES
        .iter()
        .find(|(method, uri, _)| *method == req.method && *uri == path)
        .map(|(_, _, op)| *op)
}

/// DELETE operations carry parameters in the query string (botocore
/// `location: querystring` members); POST carries a JSON body.
fn collect_params(req: &Request, body: Value) -> Value {
    if req.method == "DELETE" && !req.query_string.is_empty() {
        let map: BTreeMap<String, Vec<String>> = crate::proto::query_map(&req.query_string);
        let mut obj = Map::new();
        for (k, vals) in map {
            if vals.len() == 1 {
                obj.insert(k, json!(vals[0]));
            } else {
                obj.insert(k, json!(vals));
            }
        }
        return Value::Object(obj);
    }
    // Merge query parameters over the body (both may carry params for RAM).
    let mut merged = body;
    if !req.query_string.is_empty() {
        let map: BTreeMap<String, Vec<String>> = crate::proto::query_map(&req.query_string);
        if let Some(o) = merged.as_object_mut() {
            for (k, vals) in map {
                o.insert(k, json!(vals.first().cloned().unwrap_or_default()));
            }
        }
    }
    merged
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "ram" {
        return None;
    }
    let Some(op) = wire_operation(req) else {
        let e = json!({
            "__type": "NotImplemented",
            "message": format!("The {} action has not been implemented", req.operation),
        });
        return Some(Response::json(
            501,
            serde_json::to_vec(&e).unwrap_or_default(),
        ));
    };
    let body = crate::proto::parse_json_body(&req.body);
    let params = collect_params(req, body);

    let slot = state();
    let mut st = slot.write();
    let key = (req.account, req.region.clone());
    let s = st.entry(key).or_insert_with(|| State {
        managed_permissions: seed_permissions(),
        ..Default::default()
    });
    Some(handle(s, op, &params, req))
}

fn find_share(st: &State, arn: &str) -> Result<usize, Response> {
    st.shares
        .iter()
        .position(|s| s.arn == arn)
        .ok_or_else(|| unknown_resource(format!("ResourceShare {arn} could not be found.")))
}

fn find_permission(st: &State, arn: &str) -> Result<usize, Response> {
    st.managed_permissions
        .iter()
        .position(|p| p.arn == arn)
        .ok_or_else(|| unknown_resource(format!("Permission {arn} could not be found.")))
}

fn find_invitation(st: &State, arn: &str) -> Result<usize, Response> {
    st.invitations
        .iter()
        .position(|i| i.arn == arn)
        .ok_or_else(|| {
            unknown_resource(format!("ResourceShareInvitation {arn} could not be found."))
        })
}

fn permission_arn(region: &str, account: u64, name: &str) -> String {
    format!(
        "arn:{}:ram:{}:{}:permission/{name}",
        partition(region),
        region,
        account
    )
}

/// Association record as moto renders it in associate/disassociate/
/// GetResourceShareAssociations responses (`external` always false).
fn association(
    share: &Share,
    entity: &str,
    association_type: &str,
    status: &str,
    last_updated: f64,
) -> Value {
    json!({
        "resourceShareArn": share.arn,
        "resourceShareName": share.name,
        "associatedEntity": entity,
        "associationType": association_type,
        "status": status,
        "creationTime": share.creation_time,
        "lastUpdatedTime": last_updated,
        "external": false,
    })
}

fn valid_association_status(status: Option<&str>) -> Result<(), Response> {
    if let Some(status) = status {
        if !matches!(
            status,
            "ASSOCIATING" | "ASSOCIATED" | "FAILED" | "DISASSOCIATING" | "DISASSOCIATED"
        ) {
            return Err(invalid_parameter(format!(
                "{status} is not a valid association status."
            )));
        }
    }
    Ok(())
}

fn validate_owner(owner: Option<&str>) -> Result<(), Response> {
    match owner {
        Some("SELF") => Ok(()),
        Some("OTHER-ACCOUNTS") => Err(not_implemented(
            "Value 'OTHER-ACCOUNTS' for parameter 'resourceOwner' not implemented.",
        )),
        other => Err(invalid_parameter(format!(
            "{} is not a valid resource owner. Specify either SELF or OTHER-ACCOUNTS and try again.",
            other.unwrap_or("None")
        ))),
    }
}

fn handle(st: &mut State, op: &str, p: &Value, req: &Request) -> Response {
    let region = req.region.clone();
    let account = req.account;
    let org = crate::services::organizations::org_snapshot(account, &region);

    match op {
        // ---------------------------------------------------------------
        // Resource shares
        // ---------------------------------------------------------------
        "CreateResourceShare" => {
            let principals = param_list(p, "principals");
            let resource_arns = param_list(p, "resourceArns");
            if let Err(e) = validate_principals(&org, &principals) {
                return e;
            }
            if let Err(e) = validate_resource_arns(&resource_arns) {
                return e;
            }
            let share = Share {
                arn: format!(
                    "arn:{}:ram:{}:{}:resource-share/{}",
                    partition(&region),
                    region,
                    account,
                    uuid::Uuid::new_v4()
                ),
                name: Some(param_str(p, "name").unwrap_or_default().to_string()),
                allow_external: param_bool(p, "allowExternalPrincipals", true),
                creation_time: now_unix(),
                last_updated: now_unix(),
                status: "ACTIVE",
                principals: principals.clone(),
                resource_arns: resource_arns.clone(),
                tags: param_tags(p, "tags"),
                permission_arns: Vec::new(),
            };
            let describe = share.describe(account);
            st.shares.push(share);
            ok(&json!({"resourceShare": describe}))
        }
        "GetResourceShares" => {
            if let Err(e) = validate_owner(param_str(p, "resourceOwner")) {
                return e;
            }
            let shares: Vec<Value> = st.shares.iter().map(|s| s.describe(account)).collect();
            ok(&json!({"resourceShares": shares}))
        }
        "UpdateResourceShare" => {
            let arn = param_str(p, "resourceShareArn").unwrap_or_default();
            let idx = match find_share(st, arn) {
                Ok(i) => i,
                Err(e) => return e,
            };
            let allow = param_bool(p, "allowExternalPrincipals", true);
            let name = param_str(p, "name").map(str::to_string);
            st.shares[idx].allow_external = allow;
            st.shares[idx].name = name;
            st.shares[idx].last_updated = now_unix();
            ok(&json!({"resourceShare": st.shares[idx].describe(account)}))
        }
        "DeleteResourceShare" => {
            let arn = param_str(p, "resourceShareArn").unwrap_or_default();
            let idx = match find_share(st, arn) {
                Ok(i) => i,
                Err(e) => return e,
            };
            st.shares[idx].status = "DELETED";
            st.shares[idx].last_updated = now_unix();
            ok(&json!({"returnValue": true}))
        }
        "EnableSharingWithAwsOrganization" => {
            if org.is_none() {
                return conflict(
                    "Unable to enable sharing with AWS Organizations. \
                     Received AccessDeniedException from AWSOrganizations with the \
                     following error message: You don't have permissions to access this resource.",
                );
            }
            ok(&json!({"returnValue": true}))
        }

        // ---------------------------------------------------------------
        // Associations
        // ---------------------------------------------------------------
        "GetResourceShareAssociations" => {
            let association_type = param_str(p, "associationType").unwrap_or("None");
            if association_type != "PRINCIPAL" && association_type != "RESOURCE" {
                return invalid_parameter(format!(
                    "{association_type} is not a valid association type. \
                     Specify either PRINCIPAL or RESOURCE and try again."
                ));
            }
            if let Err(e) = valid_association_status(param_str(p, "associationStatus")) {
                return e;
            }
            let resource_arn = param_str(p, "resourceArn");
            let principal = param_str(p, "principal");
            if association_type == "PRINCIPAL" && resource_arn.is_some() {
                return invalid_parameter(
                    "You cannot specify a resource ARN when the association type is PRINCIPAL.",
                );
            }
            if association_type == "RESOURCE" && principal.is_some() {
                return invalid_parameter(
                    "You cannot specify a principal when the association type is RESOURCE.",
                );
            }
            let arns = param_list(p, "resourceShareArns");
            let status = param_str(p, "associationStatus").unwrap_or("ASSOCIATED");
            let mut associations = Vec::new();
            for share in &st.shares {
                if !arns.is_empty() && !arns.iter().any(|a| a == &share.arn) {
                    continue;
                }
                if association_type == "PRINCIPAL" {
                    for entity in &share.principals {
                        if let Some(p) = principal {
                            if p != entity {
                                continue;
                            }
                        }
                        associations.push(association(
                            share,
                            entity,
                            "PRINCIPAL",
                            status,
                            share.last_updated,
                        ));
                    }
                } else {
                    for entity in &share.resource_arns {
                        if let Some(r) = resource_arn {
                            if r != entity {
                                continue;
                            }
                        }
                        associations.push(association(
                            share,
                            entity,
                            "RESOURCE",
                            status,
                            share.last_updated,
                        ));
                    }
                }
            }
            ok(&json!({"resourceShareAssociations": associations}))
        }
        "AssociateResourceShare" => {
            let arn = param_str(p, "resourceShareArn").unwrap_or_default();
            let idx = match find_share(st, arn) {
                Ok(i) => i,
                Err(e) => return e,
            };
            let resource_arns = param_list(p, "resourceArns");
            let principals = param_list(p, "principals");
            if let Err(e) = validate_principals(&org, &principals) {
                return e;
            }
            if let Err(e) = validate_resource_arns(&resource_arns) {
                return e;
            }
            let mut associations = Vec::new();
            for resource in &resource_arns {
                st.shares[idx].resource_arns.push(resource.clone());
                associations.push(association(
                    &st.shares[idx],
                    resource,
                    "RESOURCE",
                    "ASSOCIATED",
                    now_unix(),
                ));
            }
            for principal in &principals {
                st.shares[idx].principals.push(principal.clone());
                associations.push(association(
                    &st.shares[idx],
                    principal,
                    "PRINCIPAL",
                    "ASSOCIATED",
                    now_unix(),
                ));
            }
            ok(&json!({"resourceShareAssociations": associations}))
        }
        "DisassociateResourceShare" => {
            let arn = param_str(p, "resourceShareArn").unwrap_or_default();
            let idx = match find_share(st, arn) {
                Ok(i) => i,
                Err(e) => return e,
            };
            let resource_arns = param_list(p, "resourceArns");
            let principals = param_list(p, "principals");
            let mut associations = Vec::new();
            for resource in &resource_arns {
                st.shares[idx].resource_arns.retain(|r| r != resource);
                associations.push(association(
                    &st.shares[idx],
                    resource,
                    "RESOURCE",
                    "DISASSOCIATED",
                    now_unix(),
                ));
            }
            for principal in &principals {
                st.shares[idx].principals.retain(|x| x != principal);
                associations.push(association(
                    &st.shares[idx],
                    principal,
                    "PRINCIPAL",
                    "DISASSOCIATED",
                    now_unix(),
                ));
            }
            ok(&json!({"resourceShareAssociations": associations}))
        }
        "AssociateResourceSharePermission" => {
            let arn = param_str(p, "resourceShareArn").unwrap_or_default();
            let idx = match find_share(st, arn) {
                Ok(i) => i,
                Err(e) => return e,
            };
            let permission_arn = param_str(p, "permissionArn").unwrap_or_default();
            if let Err(e) = find_permission(st, permission_arn) {
                return e;
            }
            if !st.shares[idx]
                .permission_arns
                .iter()
                .any(|a| a == permission_arn)
            {
                st.shares[idx]
                    .permission_arns
                    .push(permission_arn.to_string());
            }
            ok(&json!({"returnValue": true}))
        }
        "DisassociateResourceSharePermission" => {
            let arn = param_str(p, "resourceShareArn").unwrap_or_default();
            let idx = match find_share(st, arn) {
                Ok(i) => i,
                Err(e) => return e,
            };
            let permission_arn = param_str(p, "permissionArn").unwrap_or_default();
            st.shares[idx]
                .permission_arns
                .retain(|a| a != permission_arn);
            ok(&json!({"returnValue": true}))
        }

        // ---------------------------------------------------------------
        // Managed permissions
        // ---------------------------------------------------------------
        "CreatePermission" => {
            let name = param_str(p, "name").unwrap_or_default().to_string();
            let mut permission = Permission {
                name: Some(name),
                arn: permission_arn(&region, account, param_str(p, "name").unwrap_or_default()),
                resource_type: param_str(p, "resourceType").unwrap_or_default().to_string(),
                version: "1".into(),
                default_version: true,
                status: "ATTACHABLE".into(),
                is_resource_type_default: false,
                permission_type: "CUSTOMER_MANAGED",
                creation_time: now_str(),
                last_updated: now_str(),
                policy_template: param_str(p, "policyTemplate").map(str::to_string),
                versions: Vec::new(),
                tags: param_tags(p, "tags"),
            };
            permission.versions.push(PermissionVersion {
                version: "1".into(),
                default_version: true,
                creation_time: permission.creation_time.clone(),
                status: "ATTACHABLE".into(),
            });
            let describe = permission.describe();
            st.managed_permissions.push(permission);
            ok(&json!({"permission": describe}))
        }
        "CreatePermissionVersion" => {
            let arn = param_str(p, "permissionArn").unwrap_or_default();
            let idx = match find_permission(st, arn) {
                Ok(i) => i,
                Err(e) => return e,
            };
            let template = param_str(p, "policyTemplate").unwrap_or_default();
            let detail = st.managed_permissions[idx].create_version(template);
            ok(&json!({"permission": detail}))
        }
        "DeletePermission" => {
            let arn = param_str(p, "permissionArn").unwrap_or_default();
            let idx = match find_permission(st, arn) {
                Ok(i) => i,
                Err(e) => return e,
            };
            if st.managed_permissions[idx].permission_type == "AWS_MANAGED" {
                return invalid_parameter("You cannot delete an AWS managed permission.");
            }
            st.managed_permissions.remove(idx);
            ok(&json!({"returnValue": true, "permissionStatus": "DELETED"}))
        }
        "DeletePermissionVersion" => {
            let arn = param_str(p, "permissionArn").unwrap_or_default();
            let idx = match find_permission(st, arn) {
                Ok(i) => i,
                Err(e) => return e,
            };
            let Some(version) = param_i64(p, "permissionVersion") else {
                return invalid_parameter("permissionVersion must be an integer.");
            };
            st.managed_permissions[idx]
                .versions
                .retain(|v| v.version != version.to_string());
            ok(&json!({"returnValue": true, "permissionStatus": "DELETED"}))
        }
        "GetPermission" => {
            let arn = param_str(p, "permissionArn").unwrap_or_default();
            let idx = match find_permission(st, arn) {
                Ok(i) => i,
                Err(e) => return e,
            };
            ok(&json!({
                "permission": st.managed_permissions[idx].describe_detail(param_i64(p, "permissionVersion"))
            }))
        }
        "SetDefaultPermissionVersion" => {
            let arn = param_str(p, "permissionArn").unwrap_or_default();
            let idx = match find_permission(st, arn) {
                Ok(i) => i,
                Err(e) => return e,
            };
            let Some(version) = param_i64(p, "permissionVersion") else {
                return invalid_parameter("permissionVersion must be an integer.");
            };
            let vstr = version.to_string();
            for v in &mut st.managed_permissions[idx].versions {
                v.default_version = v.version == vstr;
            }
            st.managed_permissions[idx].version = vstr;
            st.managed_permissions[idx].default_version = true;
            ok(&json!({"returnValue": true}))
        }
        "ListPermissionVersions" => {
            let arn = param_str(p, "permissionArn").unwrap_or_default();
            let idx = match find_permission(st, arn) {
                Ok(i) => i,
                Err(e) => return e,
            };
            ok(&json!({"permissions": st.managed_permissions[idx].list_versions()}))
        }
        "ListResourceSharePermissions" => {
            let arn = param_str(p, "resourceShareArn").unwrap_or_default();
            let idx = match find_share(st, arn) {
                Ok(i) => i,
                Err(e) => return e,
            };
            let mut permissions = Vec::new();
            for perm_arn in &st.shares[idx].permission_arns {
                if let Ok(i) = find_permission(st, perm_arn) {
                    permissions.push(st.managed_permissions[i].describe());
                }
            }
            ok(&json!({"permissions": permissions}))
        }
        "ListPermissions" => {
            let resource_type = param_str(p, "resourceType");
            if let Some(rt) = resource_type {
                // moto: the service segment is case sensitive, the rest is not
                // (checks service code == requested and the whole string equal
                // case-insensitively before filtering).
                let svc = rt.split(':').next().unwrap_or("");
                let known = st.managed_permissions.iter().any(|perm| {
                    svc == perm.resource_type.split(':').next().unwrap_or("")
                        && perm.resource_type.to_lowercase() == rt.to_lowercase()
                });
                if !known {
                    return invalid_parameter(format!("Invalid resource type: {rt}"));
                }
            }
            let mut permissions: Vec<Value> = match resource_type {
                Some(rt) => st
                    .managed_permissions
                    .iter()
                    .filter(|perm| perm.resource_type.to_lowercase() == rt.to_lowercase())
                    .map(Permission::describe)
                    .collect(),
                None => st
                    .managed_permissions
                    .iter()
                    .map(Permission::describe)
                    .collect(),
            };
            let permission_type = param_str(p, "permissionType").unwrap_or("ALL");
            let relation: &[(&str, &str)] =
                &[("AWS", "AWS_MANAGED"), ("LOCAL", "CUSTOMER_MANAGED")];
            if !matches!(permission_type, "ALL" | "AWS" | "LOCAL") {
                return invalid_parameter(format!(
                    "{permission_type} is not a valid scope. Must be one of: ALL, AWS, LOCAL."
                ));
            }
            let wanted = relation.iter().find(|(scope, _)| *scope == permission_type);
            if let Some((_, managed_kwarg)) = wanted {
                permissions.retain(|perm| perm["permissionType"] == json!(managed_kwarg));
            }
            ok(&json!({"permissions": permissions}))
        }
        "ListPermissionAssociations" => {
            let permission_arn_filter = param_str(p, "permissionArn");
            let mut permissions = Vec::new();
            for share in &st.shares {
                for perm_arn in &share.permission_arns {
                    if let Some(filter) = permission_arn_filter {
                        if filter != perm_arn {
                            continue;
                        }
                    }
                    permissions.push(json!({
                        "arn": perm_arn,
                        "permissionVersion": "1",
                        "defaultVersion": true,
                        "resourceShareArn": share.arn,
                        "status": "ASSOCIATED",
                        "featureSet": "STANDARD",
                        "lastUpdatedTime": share.last_updated,
                        "resourceType": "",
                    }));
                }
            }
            ok(&json!({"permissions": permissions}))
        }
        "ListReplacePermissionAssociationsWork" => {
            let work_ids = param_list(p, "workIds");
            let status = param_str(p, "status");
            let items: Vec<Value> = st
                .replace_work_items
                .iter()
                .filter(|w| {
                    work_ids.is_empty()
                        || work_ids
                            .iter()
                            .any(|id| Some(id.as_str()) == w["id"].as_str())
                })
                .filter(|w| status.is_none_or(|s| w["status"] == json!(s)))
                .cloned()
                .collect();
            ok(&json!({"replacePermissionAssociationsWorks": items}))
        }
        "ReplacePermissionAssociations" => {
            let from_arn = param_str(p, "fromPermissionArn").unwrap_or_default();
            let to_arn = param_str(p, "toPermissionArn").unwrap_or_default();
            if let Err(e) = find_permission(st, from_arn) {
                return e;
            }
            if let Err(e) = find_permission(st, to_arn) {
                return e;
            }
            let now = now_str();
            let work_item = json!({
                "id": uuid::Uuid::new_v4().to_string(),
                "fromPermissionArn": from_arn,
                "toPermissionArn": to_arn,
                "status": "COMPLETED",
                "creationTime": now,
                "lastUpdatedTime": now,
            });
            st.replace_work_items.push(work_item.clone());
            for share in &mut st.shares {
                for arn in &mut share.permission_arns {
                    if arn == from_arn {
                        *arn = to_arn.to_string();
                    }
                }
            }
            ok(&json!({"replacePermissionAssociationsWork": work_item}))
        }
        "PromotePermissionCreatedFromPolicy" => {
            let arn = param_str(p, "permissionArn").unwrap_or_default();
            let idx = match find_permission(st, arn) {
                Ok(i) => i,
                Err(e) => return e,
            };
            st.managed_permissions[idx].name = param_str(p, "name").map(str::to_string);
            st.managed_permissions[idx].permission_type = "CUSTOMER_MANAGED";
            ok(&json!({"permission": st.managed_permissions[idx].describe()}))
        }
        "PromoteResourceShareCreatedFromPolicy" => {
            let arn = param_str(p, "resourceShareArn").unwrap_or_default();
            if let Err(e) = find_share(st, arn) {
                return e;
            }
            ok(&json!({"returnValue": true}))
        }

        // ---------------------------------------------------------------
        // Invitations
        // ---------------------------------------------------------------
        "GetResourceShareInvitations" => {
            let invitation_arns = param_list(p, "resourceShareInvitationArns");
            let share_arns = param_list(p, "resourceShareArns");
            let invitations: Vec<Value> = st
                .invitations
                .iter()
                .filter(|i| {
                    invitation_arns.is_empty() || invitation_arns.iter().any(|a| a == &i.arn)
                })
                .filter(|i| share_arns.is_empty() || share_arns.iter().any(|a| a == &i.share_arn))
                .map(Invitation::describe)
                .collect();
            ok(&json!({"resourceShareInvitations": invitations}))
        }
        "AcceptResourceShareInvitation" | "RejectResourceShareInvitation" => {
            let arn = param_str(p, "resourceShareInvitationArn").unwrap_or_default();
            let idx = match find_invitation(st, arn) {
                Ok(i) => i,
                Err(e) => return e,
            };
            st.invitations[idx].status = if op == "AcceptResourceShareInvitation" {
                "ACCEPTED"
            } else {
                "REJECTED"
            };
            ok(&json!({"resourceShareInvitation": st.invitations[idx].describe()}))
        }
        "ListPendingInvitationResources" => {
            let arn = param_str(p, "resourceShareInvitationArn").unwrap_or_default();
            let invite_idx = match find_invitation(st, arn) {
                Ok(i) => i,
                Err(e) => return e,
            };
            let share_arn = st.invitations[invite_idx].share_arn.clone();
            let mut resources = Vec::new();
            if let Some(share) = st.shares.iter().find(|s| s.arn == share_arn) {
                for res_arn in &share.resource_arns {
                    resources.push(json!({
                        "arn": res_arn,
                        "type": resource_type_of(res_arn),
                        "resourceShareArn": share.arn,
                        "resourceGroupArn": Value::Null,
                        "status": "AVAILABLE",
                        "creationTime": share.creation_time,
                        "lastUpdatedTime": share.last_updated,
                    }));
                }
            }
            ok(&json!({"resources": resources}))
        }

        // ---------------------------------------------------------------
        // Policies / resources / principals / sources
        // ---------------------------------------------------------------
        "GetResourcePolicies" => ok(&json!({"policies": []})),
        "ListSourceAssociations" => ok(&json!({"sourceAssociations": []})),
        "ListPrincipals" => {
            if let Err(e) = validate_owner(param_str(p, "resourceOwner")) {
                return e;
            }
            let share_arns = param_list(p, "resourceShareArns");
            let principals = param_list(p, "principals");
            let resource_arn = param_str(p, "resourceArn").map(str::to_string);
            let mut result = Vec::new();
            for share in &st.shares {
                if !share_arns.is_empty() && !share_arns.iter().any(|a| a == &share.arn) {
                    continue;
                }
                if let Some(ref r) = resource_arn {
                    if !share.resource_arns.iter().any(|a| a == r) {
                        continue;
                    }
                }
                for principal in &share.principals {
                    if !principals.is_empty() && !principals.iter().any(|x| x == principal) {
                        continue;
                    }
                    result.push(json!({
                        "id": principal,
                        "resourceShareArn": share.arn,
                        "creationTime": share.creation_time,
                        "lastUpdatedTime": share.last_updated,
                        "external": !is_12_digits(principal),
                    }));
                }
            }
            ok(&json!({"principals": result}))
        }
        "ListResources" => {
            if let Err(e) = validate_owner(param_str(p, "resourceOwner")) {
                return e;
            }
            let share_arns = param_list(p, "resourceShareArns");
            let principal = param_str(p, "principal");
            let resource_arns_filter = param_list(p, "resourceArns");
            let resource_type = param_str(p, "resourceType");
            let mut result = Vec::new();
            for share in &st.shares {
                if !share_arns.is_empty() && !share_arns.iter().any(|a| a == &share.arn) {
                    continue;
                }
                if let Some(p) = principal {
                    if !share.principals.iter().any(|x| x == p) {
                        continue;
                    }
                }
                for res_arn in &share.resource_arns {
                    if !resource_arns_filter.is_empty()
                        && !resource_arns_filter.iter().any(|x| x == res_arn)
                    {
                        continue;
                    }
                    let kind = resource_type_of(res_arn);
                    if let Some(rt) = resource_type {
                        if kind != rt {
                            continue;
                        }
                    }
                    result.push(json!({
                        "arn": res_arn,
                        "type": kind,
                        "resourceShareArn": share.arn,
                        "resourceGroupArn": Value::Null,
                        "status": "AVAILABLE",
                        "creationTime": share.creation_time,
                        "lastUpdatedTime": share.last_updated,
                    }));
                }
            }
            ok(&json!({"resources": result}))
        }

        // ---------------------------------------------------------------
        // Tags
        // ---------------------------------------------------------------
        "TagResource" | "UntagResource" => {
            let arn = param_str(p, "resourceShareArn")
                .map(str::to_string)
                .or_else(|| param_str(p, "resourceArn").map(str::to_string));
            let Some(arn) = arn else {
                return invalid_parameter(
                    "You must specify either resourceShareArn or resourceArn.",
                );
            };
            if let Some(share_pos) = st.shares.iter().position(|s| s.arn == arn) {
                if op == "TagResource" {
                    let tags = param_tags(p, "tags");
                    for (key, value) in tags {
                        match st.shares[share_pos]
                            .tags
                            .iter_mut()
                            .find(|(k, _)| k == &key)
                        {
                            Some(entry) => entry.1 = value,
                            None => st.shares[share_pos].tags.push((key, value)),
                        }
                    }
                } else {
                    let keys = param_list(p, "tagKeys");
                    st.shares[share_pos].tags.retain(|(k, _)| !keys.contains(k));
                }
                return ok(&json!({}));
            }
            if let Ok(perm_idx) = find_permission(st, &arn) {
                if op == "TagResource" {
                    let tags = param_tags(p, "tags");
                    for (key, value) in tags {
                        match st.managed_permissions[perm_idx]
                            .tags
                            .iter_mut()
                            .find(|(k, _)| k == &key)
                        {
                            Some(entry) => entry.1 = value,
                            None => st.managed_permissions[perm_idx].tags.push((key, value)),
                        }
                    }
                } else {
                    let keys = param_list(p, "tagKeys");
                    st.managed_permissions[perm_idx]
                        .tags
                        .retain(|(k, _)| !keys.contains(k));
                }
                return ok(&json!({}));
            }
            unknown_resource(format!("Resource {arn} could not be found."))
        }

        // ---------------------------------------------------------------
        "ListResourceTypes" => {
            let scope = param_str(p, "resourceRegionScope").unwrap_or("ALL");
            if !matches!(scope, "ALL" | "REGIONAL" | "GLOBAL") {
                return invalid_parameter(format!(
                    "{scope} is not a valid resource region \
                     scope value. Specify a valid value and try again."
                ));
            }
            let mut types = Vec::new();
            for (resource_type, service_name, region_scope) in PROVIDED_RESOURCE_TYPES {
                if scope != "ALL" && *region_scope != scope {
                    continue;
                }
                types.push(json!({
                    "resourceType": resource_type,
                    "serviceName": service_name,
                    "resourceRegionScope": region_scope,
                }));
            }
            ok(&json!({"resourceTypes": types}))
        }

        _ => {
            let e = json!({
                "__type": "NotImplemented",
                "message": format!("The {op} action has not been implemented"),
            });
            Response::json(501, serde_json::to_vec(&e).unwrap_or_default())
        }
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn mk(_op: &str, method: &str, path: &str, body: Value) -> Request {
        Request {
            service: "ram".into(),
            operation: String::new(),
            method: method.into(),
            path: path.into(),
            query_string: String::new(),
            headers: Default::default(),
            body: serde_json::to_vec(&body)
                .map(Bytes::from)
                .unwrap_or_default(),
            account: 123456789012,
            region: "us-east-1".into(),
        }
    }

    fn body_of(resp: &Response) -> Value {
        serde_json::from_slice(&resp.body).unwrap()
    }

    /// Runs dispatch on a fresh slot so tests do not interfere with server
    /// state. Uniqueness comes from the per-test account id.
    fn run(req: &Request) -> Response {
        dispatch(req).unwrap()
    }

    #[test]
    fn share_lifecycle_round_trip() {
        let mut req = mk(
            "CreateResourceShare",
            "POST",
            "/createresourceshare",
            json!({
                "name": "compat-share-1",
                "allowExternalPrincipals": true
            }),
        );
        req.account = 111000111001;
        let created = body_of(&run(&req));
        let share = &created["resourceShare"];
        assert_eq!(share["name"], json!("compat-share-1"));
        assert_eq!(share["allowExternalPrincipals"], json!(true));
        assert_eq!(share["status"], json!("ACTIVE"));
        let arn = share["resourceShareArn"].as_str().unwrap().to_string();
        assert!(arn.starts_with("arn:aws:ram:us-east-1:111000111001:resource-share/"));
        assert!(share.get("creationTime").is_some());
        assert!(share.get("lastUpdatedTime").is_some());
        assert!(share.get("owningAccountId").is_some());
        assert_eq!(share.get("featureSet").and_then(Value::as_str), Some("STANDARD"), "moto emits featureSet");

        // list (SELF)
        req = mk(
            "GetResourceShares",
            "POST",
            "/getresourceshares",
            json!({"resourceOwner": "SELF"}),
        );
        req.account = 111000111001;
        let listed = body_of(&run(&req));
        let arns: Vec<&str> = listed["resourceShares"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s["resourceShareArn"].as_str().unwrap())
            .collect();
        assert!(arns.contains(&arn.as_str()));

        // update name
        req = mk(
            "UpdateResourceShare",
            "POST",
            "/updateresourceshare",
            json!({
                "resourceShareArn": arn,
                "name": "renamed"
            }),
        );
        req.account = 111000111001;
        let updated = body_of(&run(&req));
        assert_eq!(updated["resourceShare"]["name"], json!("renamed"));
        assert_eq!(updated["resourceShare"]["resourceShareArn"], json!(arn));

        // delete
        req = mk(
            "DeleteResourceShare",
            "DELETE",
            "/deleteresourceshare",
            json!({}),
        );
        req.account = 111000111001;
        req.query_string = format!("resourceShareArn={}", urlencoding::encode(&arn));
        assert_eq!(body_of(&run(&req))["returnValue"], json!(true));

        // delete again: moto keeps the share (status DELETED) so returnValue true
        assert_eq!(body_of(&run(&req))["returnValue"], json!(true));
    }

    #[test]
    fn unknown_share_arn_errors() {
        let mut req = mk(
            "DeleteResourceShare",
            "DELETE",
            "/deleteresourceshare",
            json!({}),
        );
        req.query_string = "resourceShareArn=arn:aws:ram:us-east-1:123456789012:resource-share/00000000-0000-0000-0000-000000000000"
            .to_string();
        let resp = run(&req);
        assert_eq!(resp.status, 400);
        assert_eq!(body_of(&resp)["__type"], json!("UnknownResourceException"));

        let mut req = mk(
            "UpdateResourceShare",
            "POST",
            "/updateresourceshare",
            json!({
                "resourceShareArn": "arn:aws:ram:us-east-1:123456789012:resource-share/00000000-0000-0000-0000-000000000000",
                "name": "nope"
            }),
        );
        req.account = 111000111002;
        let resp = run(&req);
        assert_eq!(resp.status, 400);
        assert_eq!(body_of(&resp)["__type"], json!("UnknownResourceException"));
    }

    #[test]
    fn owner_validation() {
        let mut req = mk(
            "GetResourceShares",
            "POST",
            "/getresourceshares",
            json!({"resourceOwner": "OTHER-ACCOUNTS"}),
        );
        req.account = 111000111003;
        let resp = run(&req);
        assert_eq!(resp.status, 501);
        assert_eq!(body_of(&resp)["__type"], json!("NotImplemented"));

        let mut req = mk(
            "GetResourceShares",
            "POST",
            "/getresourceshares",
            json!({"resourceOwner": "bogus"}),
        );
        req.account = 111000111004;
        let resp = run(&req);
        assert_eq!(resp.status, 400);
        assert_eq!(body_of(&resp)["__type"], json!("InvalidParameterException"));
    }

    #[test]
    fn principal_and_resource_validation() {
        let mut req = mk(
            "CreateResourceShare",
            "POST",
            "/createresourceshare",
            json!({
                "name": "bad-principal",
                "principals": ["definitely not a principal"]
            }),
        );
        req.account = 111000111004;
        let resp = run(&req);
        let b = body_of(&resp);
        assert_eq!(resp.status, 400);
        assert_eq!(b["__type"], json!("InvalidParameterException"));
        assert!(b["message"]
            .as_str()
            .unwrap()
            .starts_with("Principal ID definitely not a principal is malformed"));

        // A shareable resource ARN is accepted and recorded.
        let mut req = mk(
            "CreateResourceShare",
            "POST",
            "/createresourceshare",
            json!({
                "name": "with-arns",
                "resourceArns": ["arn:aws:ec2:us-east-1:111000111005:subnet/subnet-123"]
            }),
        );
        req.account = 111000111005;
        let created = body_of(&run(&req));
        assert_eq!(created["resourceShare"]["status"], json!("ACTIVE"));
        let arn = created["resourceShare"]["resourceShareArn"]
            .as_str()
            .unwrap()
            .to_string();

        req = mk(
            "ListResources",
            "POST",
            "/listresources",
            json!({"resourceOwner": "SELF"}),
        );
        req.account = 111000111005;
        let listed = body_of(&run(&req));
        let mut found = false;
        for r in listed["resources"].as_array().unwrap() {
            if r["resourceShareArn"].as_str().unwrap() == arn {
                found = true;
                assert_eq!(r["type"], json!("subnet"));
                assert_eq!(r["status"], json!("AVAILABLE"));
            }
        }
        assert!(found);

        // Non-shareable resource kind is rejected (moto checks pieces).
        let mut req = mk(
            "CreateResourceShare",
            "POST",
            "/createresourceshare",
            json!({
                "name": "bad-kind",
                "resourceArns": ["arn:aws:ec2:us-east-1:111000111006:notshareable/x"]
            }),
        );
        req.account = 111000111006;
        let resp = run(&req);
        assert_eq!(resp.status, 400);
        assert_eq!(body_of(&resp)["__type"], json!("MalformedArnException"));
        assert_eq!(
            body_of(&resp)["message"],
            json!("You cannot share the selected resource type.")
        );
    }

    #[test]
    fn permission_lifecycle() {
        let mut req = mk(
            "CreatePermission",
            "POST",
            "/createpermission",
            json!({
                "name": "perm-name-1",
                "resourceType": "ec2:Subnet",
                "policyTemplate": "{\"Effect\": \"Allow\"}"
            }),
        );
        req.account = 111000111007;
        let created = body_of(&run(&req))["permission"].clone();
        assert_eq!(created["name"], json!("perm-name-1"));
        assert_eq!(created["resourceType"], json!("ec2:Subnet"));
        assert_eq!(created["version"], json!("1"));
        assert_eq!(created["defaultVersion"], json!(true));
        assert_eq!(created["permissionType"], json!("CUSTOMER_MANAGED"));
        let arn = created["arn"].as_str().unwrap().to_string();
        assert_eq!(
            arn,
            "arn:aws:ram:us-east-1:111000111007:permission/perm-name-1"
        );

        // Not found on wrong arn get_permission.
        req = mk(
            "GetPermission",
            "POST",
            "/getpermission",
            json!({
                "permissionArn": "arn:aws:ram::123456789012:permission/00000000-0000-0000-0000-000000000000"
            }),
        );
        req.account = 111000111007;
        let resp = run(&req);
        assert_eq!(resp.status, 400);
        assert_eq!(body_of(&resp)["__type"], json!("UnknownResourceException"));

        // version 2
        req = mk(
            "CreatePermissionVersion",
            "POST",
            "/createpermissionversion",
            json!({"permissionArn": arn, "policyTemplate": "{}"}),
        );
        req.account = 111000111007;
        let v2 = body_of(&run(&req))["permission"].clone();
        assert_eq!(v2["version"], json!("2"));
        assert_eq!(v2["arn"], json!(arn));
        assert_eq!(v2["defaultVersion"].as_bool(), Some(false));
        assert_eq!(v2["permission"], json!("{}"));

        req = mk(
            "SetDefaultPermissionVersion",
            "POST",
            "/setdefaultpermissionversion",
            json!({"permissionArn": arn, "permissionVersion": 2}),
        );
        req.account = 111000111007;
        assert_eq!(body_of(&run(&req))["returnValue"], json!(true));

        req = mk(
            "DeletePermissionVersion",
            "DELETE",
            "/deletepermissionversion",
            json!({}),
        );
        req.account = 111000111007;
        req.query_string = format!(
            "permissionArn={}&permissionVersion=1",
            urlencoding::encode(&arn)
        );
        assert_eq!(body_of(&run(&req))["permissionStatus"], json!("DELETED"));

        req = mk("DeletePermission", "DELETE", "/deletepermission", json!({}));
        req.account = 111000111007;
        req.query_string = format!("permissionArn={}", urlencoding::encode(&arn));
        assert_eq!(body_of(&run(&req))["returnValue"], json!(true));
    }

    #[test]
    fn cannot_delete_aws_managed_permission() {
        let mut req = mk("DeletePermission", "DELETE", "/deletepermission", json!({}));
        req.query_string =
            "permissionArn=arn%3Aaws%3Aram%3A%3Aaws%3Apermission%2FAWSRAMDefaultPermissionSubnet"
                .to_string();
        let resp = run(&req);
        assert_eq!(resp.status, 400);
        assert_eq!(
            body_of(&resp)["message"],
            json!("You cannot delete an AWS managed permission.")
        );

        // ListPermissions still surfaces the AWS-managed seeds.
        req = mk(
            "ListPermissions",
            "POST",
            "/listpermissions",
            json!({"permissionType": "AWS"}),
        );
        req.account = 111000111008;
        let listed = body_of(&run(&req));
        let names: Vec<&str> = listed["permissions"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| p["name"].as_str().unwrap())
            .collect();
        assert!(names.contains(&"AWSRAMDefaultPermissionSubnet"));
        assert!(names.iter().all(|n| n.starts_with("AWSRAM")));
    }

    #[test]
    fn associate_and_disassociate_principal() {
        let mut req = mk(
            "CreateResourceShare",
            "POST",
            "/createresourceshare",
            json!({
                "name": "assoc-share"
            }),
        );
        req.account = 111000111009;
        let arn = body_of(&run(&req))["resourceShare"]["resourceShareArn"]
            .as_str()
            .unwrap()
            .to_string();

        req = mk(
            "AssociateResourceShare",
            "POST",
            "/associateresourceshare",
            json!({
                "resourceShareArn": arn.clone(),
                "principals": ["123456789012"]
            }),
        );
        req.account = 111000111009;
        let assoc = body_of(&run(&req))["resourceShareAssociations"].clone();
        let first = &assoc.as_array().unwrap()[0];
        assert_eq!(first["associationType"], json!("PRINCIPAL"));
        assert_eq!(first["status"], json!("ASSOCIATED"));
        assert_eq!(first["associatedEntity"], json!("123456789012"));
        assert!(first.get("creationTime").is_some());
        assert!(first.get("lastUpdatedTime").is_some());

        req = mk(
            "DisassociateResourceShare",
            "POST",
            "/disassociateresourceshare",
            json!({"resourceShareArn": arn, "principals": ["123456789012"]}),
        );
        req.account = 111000111009;
        let assoc = body_of(&run(&req))["resourceShareAssociations"].clone();
        assert_eq!(
            assoc.as_array().unwrap()[0]["status"],
            json!("DISASSOCIATED")
        );

        // Unknown share ARN errors out.
        let mut req = mk(
            "AssociateResourceShare",
            "POST",
            "/associateresourceshare",
            json!({"resourceShareArn": "arn:aws:ram:us-east-1:123456789012:resource-share/nope", "principals": ["123456789012"]}),
        );
        req.account = 111000111009;
        let resp = run(&req);
        assert_eq!(body_of(&resp)["__type"], json!("UnknownResourceException"));
    }

    #[test]
    fn tag_and_untag_share() {
        let mut req = mk(
            "CreateResourceShare",
            "POST",
            "/createresourceshare",
            json!({
                "name": "tagged"
            }),
        );
        req.account = 111000111010;
        let arn = body_of(&run(&req))["resourceShare"]["resourceShareArn"]
            .as_str()
            .unwrap()
            .to_string();

        req = mk(
            "TagResource",
            "POST",
            "/tagresource",
            json!({
                "resourceShareArn": arn.clone(),
                "tags": [{"key": "env", "value": "test"}]
            }),
        );
        req.account = 111000111010;
        assert_eq!(run(&req).status, 200);

        req = mk(
            "UntagResource",
            "POST",
            "/untagresource",
            json!({
                "resourceShareArn": arn,
                "tagKeys": ["env"]
            }),
        );
        req.account = 111000111010;
        assert_eq!(run(&req).status, 200);

        // Tagging an unknown resource surfaces moto's UnknownResourceException.
        let mut req = mk(
            "TagResource",
            "POST",
            "/tagresource",
            json!({
                "resourceShareArn": "arn:aws:ram:us-east-1:123456789012:resource-share/none",
                "tags": []
            }),
        );
        req.account = 111000111010;
        let resp = run(&req);
        assert_eq!(resp.status, 400);
        assert_eq!(body_of(&resp)["__type"], json!("UnknownResourceException"));
    }

    #[test]
    fn list_permissions_rejects_bad_resource_type() {
        let mut req = mk(
            "ListPermissions",
            "POST",
            "/listpermissions",
            json!({"resourceType": "ec2:Subnet"}),
        );
        req.account = 111000111011;
        // ec2:Subnet exists as a managed permission seed: exactly one match.
        let listed = body_of(&run(&req));
        assert_eq!(listed["permissions"].as_array().unwrap().len(), 1);

        let mut req = mk(
            "ListPermissions",
            "POST",
            "/listpermissions",
            json!({"resourceType": "nope:Nope"}),
        );
        req.account = 111000111011;
        let resp = run(&req);
        assert_eq!(resp.status, 400);
        assert_eq!(
            body_of(&resp)["message"],
            json!("Invalid resource type: nope:Nope")
        );
    }

    #[test]
    fn resource_types_listed() {
        let mut req = mk("ListResourceTypes", "POST", "/listresourcetypes", json!({}));
        req.account = 111000111012;
        let listed = body_of(&run(&req));
        let types = listed["resourceTypes"].as_array().unwrap();
        assert!(types.len() >= 17);
        assert!(types
            .iter()
            .any(|t| t["resourceType"] == json!("ec2:Subnet")));
        assert!(types
            .iter()
            .any(|t| t["resourceRegionScope"] == json!("GLOBAL")));

        let mut req = mk(
            "ListResourceTypes",
            "POST",
            "/listresourcetypes",
            json!({"resourceRegionScope": "bogus"}),
        );
        req.account = 111000111012;
        let resp = run(&req);
        assert_eq!(resp.status, 400);
        assert!(resp
            .headers
            .iter()
            .any(|(k, v)| k == "x-amzn-ErrorType" && v.starts_with("InvalidParameterException")));
    }

    #[test]
    fn operation_resolution_by_wire_path() {
        let mut req = mk(
            "GetResourcePolicies",
            "POST",
            "/getresourcepolicies",
            json!({}),
        );
        req.account = 111000111013;
        let resp = run(&req);
        assert_eq!(resp.status, 200);
        assert_eq!(body_of(&resp)["policies"], json!([]));

        // Unknown path answers the NotImplemented document (501).
        let mut req = mk("X", "POST", "/nosuchramoperation", json!({}));
        req.account = 111000111013;
        let resp = run(&req);
        assert_eq!(resp.status, 501);

        let mut req = mk(
            "ListSourceAssociations",
            "POST",
            "/listsourceassociations",
            json!({}),
        );
        req.account = 111000111013;
        let resp = run(&req);
        assert_eq!(resp.status, 200);
        assert_eq!(body_of(&resp)["sourceAssociations"], json!([]));
    }

    #[test]
    fn invitations_et_al() {
        let mut req = mk(
            "GetResourceShareInvitations",
            "POST",
            "/getresourceshareinvitations",
            json!({}),
        );
        req.account = 111000111014;
        let resp = run(&req);
        assert_eq!(resp.status, 200);
        assert!(resp
            .headers
            .iter()
            .any(|(k, v)| k == "Content-Type" && v.contains("json")));
        assert_eq!(body_of(&resp)["resourceShareInvitations"], json!([]));

        // Rej/accept for a fake invitation ARN render the moto error message.
        req = mk(
            "RejectResourceShareInvitation",
            "POST",
            "/rejectresourceshareinvitation",
            json!({
                "resourceShareInvitationArn":
                    "arn:aws:ram:us-east-1:123456789012:resource-share-invitation/00000000-0000-0000-0000-000000000000"
            }),
        );
        req.account = 111000111014;
        let resp = run(&req);
        assert_eq!(resp.status, 400);
        assert_eq!(body_of(&resp)["__type"], json!("UnknownResourceException"));
    }
}
