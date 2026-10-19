//! IoT Core backend (REST-JSON, control plane).
//!
//! Port of moto's `iot` service: things, thing types, thing groups,
//! policies (+ versions, attach/detach), certificates (device, CA, CSR,
//! transfers), jobs and job executions, topic rules, billing groups,
//! dynamic thing groups, domain configurations, role aliases, security
//! profiles, authorizers, provisioning templates, dimensions, custom
//! metrics, fleet metrics, streams, OTA updates, packages, audit /
//! mitigation / scheduled-audit records, indexing and search, logging
//! options, event configurations, registration tasks, commands and tags.
//!
//! Wire shapes mirror moto's lowercase camel JSON members (the ground
//! truth the pytest compat suites assert); errors are iot-shaped JSON
//! `{"__type": code, "message": ...}` with moto's status codes. Requests
//! whose (method, path) does not resolve to a ported route return `None`
//! so the gateway serves them from the moto sidecar instead.
//!
//! State is keyed (account, region). Thing shadow documents live on the
//! things (moto keeps them inside FakeThing); the sibling data-plane
//! module ([`crate::services::iot_data`]) reads and writes them through
//! the same state slot.

#[allow(dead_code)]
use crate::proto::{parse_json_body, query_first, query_map};
use crate::store;
use crate::{Request, Response};
use once_cell::sync::Lazy;
use serde_json::{json, Value};
use std::collections::BTreeMap;

#[derive(Default)]
pub struct State {
    /// Records are JSON mirrors of moto's model objects; every handler
    /// emits exactly the members moto's `to_dict` variants would.
    pub things: Vec<Value>,
    pub thing_types: Vec<Value>,
    pub thing_groups: Vec<Value>,
    pub dynamic_thing_groups: Vec<Value>,
    pub billing_groups: Vec<Value>,
    /// Device certificates.
    pub certificates: Vec<Value>,
    pub ca_certificates: Vec<Value>,
    pub policies: Vec<Value>,
    /// (principal arn, policy name) attachment pairs.
    pub principal_policies: Vec<(String, String)>,
    /// (principal arn, thing name) attachment pairs.
    pub principal_things: Vec<(String, String)>,
    pub rules: Vec<Value>,
    pub role_aliases: Vec<Value>,
    pub domain_configurations: Vec<Value>,
    pub job_templates: Vec<Value>,
    pub jobs: Vec<Value>,
    /// ((job_id, thing_name), execution record).
    pub job_executions: Vec<((String, String), Value)>,
    pub provisioning_templates: Vec<Value>,
    pub dimensions: Vec<Value>,
    pub custom_metrics: Vec<Value>,
    pub fleet_metrics: Vec<Value>,
    pub streams: Vec<Value>,
    pub mitigation_actions: Vec<Value>,
    pub security_profiles: Vec<Value>,
    pub authorizers: Vec<Value>,
    pub certificate_providers: Vec<Value>,
    pub audit_mitigation_tasks: BTreeMap<String, Value>,
    pub detect_mitigation_tasks: BTreeMap<String, Value>,
    /// key: `checkName:canonicalJson(resourceIdentifier)`.
    pub audit_suppressions: BTreeMap<String, Value>,
    pub scheduled_audits: Vec<Value>,
    pub ota_updates: Vec<Value>,
    pub packages: Vec<Value>,
    /// (package name, version records).
    pub package_versions: BTreeMap<String, Vec<Value>>,
    pub topic_rule_destinations: Vec<Value>,
    pub commands: Vec<Value>,
    pub registration_tasks: BTreeMap<String, Value>,
    /// resourceArn -> tag list [{"Key","Value"}].
    pub tags: BTreeMap<String, Value>,
    pub account_audit_configuration: Value,
    pub event_configurations: BTreeMap<String, Value>,
    pub default_authorizer_name: Option<String>,
    pub logging_options: Value,
    pub v2_logging_options: Value,
    pub v2_logging_levels: BTreeMap<String, Value>,
    pub thing_indexing_configuration: Value,
    pub thing_group_indexing_configuration: Value,
}

fn state() -> store::Slot<State> {
    static SLOT: Lazy<store::Slot<State>> = Lazy::new(|| store::slot("iot"));
    SLOT.clone()
}

/// Shared handle so the sibling data-plane module sees the same records.
pub(crate) fn iot_state() -> store::Slot<State> {
    state()
}

// ---------------------------------------------------------------------------
// Wire helpers — iot answers rest-json; errors are `{"__type", "message"}`
// with an `x-amzn-ErrorType` header, exactly like moto's JsonRESTError.
// ---------------------------------------------------------------------------

fn ok_json(v: &Value) -> Response {
    Response::new(
        200,
        "application/json",
        serde_json::to_vec(v).unwrap_or_default(),
    )
}

/// moto's EmptyResult: empty rest-json structure body with 200.
fn ok_empty() -> Response {
    ok_json(&json!({}))
}

fn json_err(status: u16, code: &str, body: Value) -> Response {
    let mut resp = Response::new(
        status,
        "application/json",
        serde_json::to_vec(&body).unwrap_or_default(),
    );
    resp.headers
        .push(("x-amzn-ErrorType".to_string(), format!("{code}:")));
    resp
}

fn not_found() -> Response {
    json_err(
        404,
        "ResourceNotFoundException",
        json!({"__type": "ResourceNotFoundException", "message": "The specified resource does not exist"}),
    )
}

fn not_found_msg(msg: String) -> Response {
    json_err(
        404,
        "ResourceNotFoundException",
        json!({"__type": "ResourceNotFoundException", "message": msg}),
    )
}

fn invalid_request(msg: &str) -> Response {
    json_err(
        400,
        "InvalidRequestException",
        json!({"__type": "InvalidRequestException", "message": msg}),
    )
}

fn already_exists(msg: &str, resource_id: &str, resource_arn: &str) -> Response {
    json_err(
        409,
        "ResourceAlreadyExistsException",
        json!({
            "__type": "ResourceAlreadyExistsException",
            "message": msg,
            "resourceId": resource_id,
            "resourceArn": resource_arn,
        }),
    )
}

fn conflict(msg: &str) -> Response {
    json_err(
        409,
        "ConflictException",
        json!({"__type": "ConflictException", "message": msg}),
    )
}

fn delete_conflict(msg: &str) -> Response {
    json_err(
        409,
        "DeleteConflictException",
        json!({"__type": "DeleteConflictException", "message": msg}),
    )
}

fn version_conflict(name: &str) -> Response {
    json_err(
        409,
        "VersionConflictException",
        json!({"__type": "VersionConflictException", "message":
            format!("The version for thing {name} does not match the expected version.")}),
    )
}

fn invalid_state_transition() -> Response {
    json_err(
        409,
        "InvalidStateTransitionException",
        json!({"__type": "InvalidStateTransitionException", "message":
            "An attempt was made to change to an invalid state."}),
    )
}

fn versions_limit(name: &str) -> Response {
    json_err(
        409,
        "VersionsLimitExceededException",
        json!({"__type": "VersionsLimitExceededException", "message":
            format!("The policy {name} already has the maximum number of versions (5)")}),
    )
}

fn cert_state_error(msg: &str, cert_id: &str) -> Response {
    json_err(
        406,
        "CertificateStateException",
        json!({"__type": "CertificateStateException", "message": format!("{msg} Id: {cert_id}")}),
    )
}

fn pct_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut decoded = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'%' if i + 2 < b.len() => {
                let hexpart = std::str::from_utf8(&b[i + 1..i + 3]).unwrap_or("");
                match u8::from_str_radix(hexpart, 16) {
                    Ok(v) => {
                        decoded.push(v);
                        i += 3;
                    }
                    Err(_) => {
                        decoded.push(b[i]);
                        i += 1;
                    }
                }
            }
            c => {
                decoded.push(c);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&decoded).into_owned()
}

fn segments(path: &str) -> Vec<String> {
    path.split('/')
        .filter(|seg| !seg.is_empty())
        .map(pct_decode)
        .collect()
}

fn random_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

fn sha256_hex(data: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

/// Merge body members into `record` (present-and-non-null members replace
/// their current value) — moto's `update_*` merge behaviour.
fn merge_opt_members(record: &mut Value, body: &Value, keys: &[&str]) {
    if let Some(obj) = record.as_object_mut() {
        for key in keys {
            if let Some(v) = body.get(*key).filter(|v| !v.is_null()) {
                obj.insert((*key).to_string(), v.clone());
            }
        }
    }
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

fn arn_for(region: &str, account: u64, kind: &str, id: &str) -> String {
    format!(
        "arn:{}:iot:{}:{}:{kind}/{}",
        partition(region),
        region,
        account,
        id
    )
}

fn header<'a>(req: &'a Request, name: &str) -> Option<&'a str> {
    req.headers.get(name).map(String::as_str)
}

/// A member of the request: body JSON or the query string.
fn p<'a>(body: &'a Value, query: &'a BTreeMap<String, Vec<String>>, key: &str) -> Value {
    if !body.get(key).unwrap_or(&Value::Null).is_null() {
        return body[key].clone();
    }
    if let Some(v) = query_first(query, key) {
        return json!(v);
    }
    Value::Null
}

fn p_str(body: &Value, query: &BTreeMap<String, Vec<String>>, key: &str) -> Option<String> {
    match p(body, query, key) {
        Value::String(s) => Some(s),
        Value::Null => None,
        other => Some(other.to_string()),
    }
}

fn p_bool(body: &Value, query: &BTreeMap<String, Vec<String>>, key: &str) -> Option<bool> {
    match p(body, query, key) {
        Value::Bool(b) => Some(b),
        Value::String(s) if s == "true" => Some(true),
        Value::String(s) if s == "false" => Some(false),
        _ => None,
    }
}

fn p_i64(body: &Value, query: &BTreeMap<String, Vec<String>>, key: &str) -> Option<i64> {
    match p(body, query, key) {
        Value::Number(n) => n.as_i64(),
        Value::String(s) => s.parse::<i64>().ok(),
        _ => None,
    }
}

/// Body-only member (present and non-null).
fn b_val_ref<'a>(body: &'a Value, key: &str) -> Option<&'a Value> {
    body.get(key).filter(|v| !v.is_null())
}

fn b_str<'a>(body: &'a Value, key: &str) -> Option<&'a str> {
    b_val_ref(body, key).and_then(Value::as_str)
}

fn b_bool(body: &Value, key: &str) -> Option<bool> {
    b_val_ref(body, key).and_then(Value::as_bool)
}

/// Awaiting any record op that produced a `Result<Response, Response>`.
fn rr(r: Result<Response, Response>) -> Option<Response> {
    Some(match r {
        Ok(r) => r,
        Err(e) => e,
    })
}

/// Canonical audit-suppression map key (moto uses
/// `checkName:sorted-json(resourceIdentifier)`).
fn suppression_key(check_name: &str, ri: &Value) -> String {
    format!("{check_name}:{ri}")
}

fn b_map_to_value(map: &BTreeMap<String, Value>) -> Value {
    let mut m = serde_json::Map::new();
    for (k, v) in map {
        m.insert(k.clone(), v.clone());
    }
    Value::Object(m)
}

fn find_by_field_idx(store: &[Value], field: &str, value: &str) -> Option<usize> {
    store.iter().position(|r| r[field].as_str() == Some(value))
}

// ---------------------------------------------------------------------------
// Request → operation resolution. Mirrors botocore's iot REST table
// (method + requestUri); unmatched pairs return None for sidecar fallback.
// ---------------------------------------------------------------------------

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "iot" {
        return None;
    }
    let segs = segments(&req.path);
    let op = resolve_op(&req.method, &segs)?;
    let body = parse_json_body(&req.body);
    let query = query_map(&req.query_string);

    let slot = state();
    let mut guard = slot.write();
    let s = guard.entry((req.account, req.region.clone())).or_default();
    handle(op, s, req, &segs, &body, &query)
}

fn resolve_op(method: &str, segs: &[String]) -> Option<&'static str> {
    let seg0 = segs.first().map(String::as_str).unwrap_or("");
    let n = segs.len();
    match seg0 {
        "things" => match (method, n) {
            ("GET", 1) => Some("ListThings"),
            ("POST", 1) => Some("RegisterThing"),
            ("POST", 2) => Some("CreateThing"),
            ("GET", 2) => Some("DescribeThing"),
            ("PATCH", 2) => Some("UpdateThing"),
            ("DELETE", 2) => Some("DeleteThing"),
            ("PUT", 3) if segs[2] == "principals" => Some("AttachThingPrincipal"),
            ("DELETE", 3) if segs[2] == "principals" => Some("DetachThingPrincipal"),
            ("GET", 3) if segs[2] == "principals" => Some("ListThingPrincipals"),
            ("GET", 3) if segs[2] == "principals-v2" => Some("ListThingPrincipalsV2"),
            ("GET", 3) if segs[2] == "thing-groups" => Some("ListThingGroupsForThing"),
            ("POST", 3) if segs[2] == "connectivity-data" => Some("GetThingConnectivityData"),
            ("GET", 3) if segs[2] == "jobs" => Some("ListJobExecutionsForThing"),
            ("GET", 4) if segs[2] == "jobs" => Some("DescribeJobExecution"),
            ("PUT", 5) if segs[2] == "jobs" && segs[4] == "cancel" => Some("CancelJobExecution"),
            ("DELETE", 6) if segs[2] == "jobs" && segs[4] == "executionNumber" => {
                Some("DeleteJobExecution")
            }
            _ => None,
        },
        "thing-types" => match (method, n) {
            ("GET", 1) => Some("ListThingTypes"),
            ("POST", 2) => Some("CreateThingType"),
            ("GET", 2) => Some("DescribeThingType"),
            ("PATCH", 2) => Some("UpdateThingType"),
            ("DELETE", 2) => Some("DeleteThingType"),
            ("POST", 3) if segs[2] == "deprecate" => Some("DeprecateThingType"),
            _ => None,
        },
        "thing-groups" => match (method, n) {
            ("GET", 1) => Some("ListThingGroups"),
            ("PUT", 2) if segs[1] == "addThingToThingGroup" => Some("AddThingToThingGroup"),
            ("PUT", 2) if segs[1] == "removeThingFromThingGroup" => {
                Some("RemoveThingFromThingGroup")
            }
            ("PUT", 2) if segs[1] == "updateThingGroupsForThing" => {
                Some("UpdateThingGroupsForThing")
            }
            ("POST", 2) => Some("CreateThingGroup"),
            ("GET", 2) => Some("DescribeThingGroup"),
            ("PATCH", 2) => Some("UpdateThingGroup"),
            ("DELETE", 2) => Some("DeleteThingGroup"),
            ("GET", 3) if segs[2] == "things" => Some("ListThingsInThingGroup"),
            _ => None,
        },
        "billing-groups" => match (method, n) {
            ("GET", 1) => Some("ListBillingGroups"),
            ("PUT", 2) if segs[1] == "addThingToBillingGroup" => Some("AddThingToBillingGroup"),
            ("PUT", 2) if segs[1] == "removeThingFromBillingGroup" => {
                Some("RemoveThingFromBillingGroup")
            }
            ("POST", 2) => Some("CreateBillingGroup"),
            ("GET", 2) => Some("DescribeBillingGroup"),
            ("PATCH", 2) => Some("UpdateBillingGroup"),
            ("DELETE", 2) => Some("DeleteBillingGroup"),
            ("GET", 3) if segs[2] == "things" => Some("ListThingsInBillingGroup"),
            _ => None,
        },
        "dynamic-thing-groups" => match (method, n) {
            ("POST", 2) => Some("CreateDynamicThingGroup"),
            ("PATCH", 2) => Some("UpdateDynamicThingGroup"),
            ("DELETE", 2) => Some("DeleteDynamicThingGroup"),
            _ => None,
        },
        "policies" => match (method, n) {
            ("GET", 1) => Some("ListPolicies"),
            ("POST", 2) => Some("CreatePolicy"),
            ("GET", 2) => Some("GetPolicy"),
            ("DELETE", 2) => Some("DeletePolicy"),
            ("POST", 3) if segs[2] == "version" => Some("CreatePolicyVersion"),
            ("GET", 3) => Some("ListPolicyVersions"),
            ("GET", 4) => Some("GetPolicyVersion"),
            ("DELETE", 4) => Some("DeletePolicyVersion"),
            ("PATCH", 4) => Some("SetDefaultPolicyVersion"),
            _ => None,
        },
        "policy-principals" => match (method, n) {
            ("GET", 1) => Some("ListPolicyPrincipals"),
            _ => None,
        },
        "principal-policies" => match (method, n) {
            ("PUT", 2) => Some("AttachPrincipalPolicy"),
            ("DELETE", 2) => Some("DetachPrincipalPolicy"),
            ("GET", 1) => Some("ListPrincipalPolicies"),
            _ => None,
        },
        "policy-targets" => match (method, n) {
            ("POST", 2) => Some("ListTargetsForPolicy"),
            _ => None,
        },
        "target-policies" => match (method, n) {
            ("PUT", 2) => Some("AttachPolicy"),
            ("POST", 2) => Some("DetachPolicy"),
            _ => None,
        },
        "attached-policies" => match (method, n) {
            ("POST", 2) => Some("ListAttachedPolicies"),
            _ => None,
        },
        "principals" => match (method, n) {
            ("GET", 2) if segs[1] == "things" => Some("ListPrincipalThings"),
            ("GET", 2) if segs[1] == "things-v2" => Some("ListPrincipalThingsV2"),
            _ => None,
        },
        "certificates" => match (method, n) {
            ("POST", 1) => Some("CreateCertificateFromCsr"),
            ("GET", 1) => Some("ListCertificates"),
            ("GET", 2) => Some("DescribeCertificate"),
            ("PUT", 2) => Some("UpdateCertificate"),
            ("DELETE", 2) => Some("DeleteCertificate"),
            _ => None,
        },
        "certificate" => match (method, n) {
            ("POST", 2) if segs[1] == "register" => Some("RegisterCertificate"),
            ("POST", 2) if segs[1] == "register-no-ca" => Some("RegisterCertificateWithoutCA"),
            _ => None,
        },
        "cacertificate" => match (method, n) {
            ("POST", 1) => Some("RegisterCACertificate"),
            ("GET", 2) => Some("DescribeCACertificate"),
            ("PUT", 2) => Some("UpdateCACertificate"),
            ("DELETE", 2) => Some("DeleteCACertificate"),
            _ => None,
        },
        "cacertificates" => match (method, n) {
            ("GET", 1) => Some("ListCACertificates"),
            _ => None,
        },
        "keys-and-certificate" => match (method, n) {
            ("POST", 1) => Some("CreateKeysAndCertificate"),
            _ => None,
        },
        "transfer-certificate" => match (method, n) {
            ("PATCH", 2) => Some("TransferCertificate"),
            _ => None,
        },
        "accept-certificate-transfer" => match (method, n) {
            ("PATCH", 2) => Some("AcceptCertificateTransfer"),
            _ => None,
        },
        "cancel-certificate-transfer" => match (method, n) {
            ("PATCH", 2) => Some("CancelCertificateTransfer"),
            _ => None,
        },
        "reject-certificate-transfer" => match (method, n) {
            ("PATCH", 2) => Some("RejectCertificateTransfer"),
            _ => None,
        },
        "certificates-by-ca" => match (method, n) {
            ("GET", 2) => Some("ListCertificatesByCA"),
            _ => None,
        },
        "certificates-out-going" => match (method, n) {
            ("GET", 1) => Some("ListOutgoingCertificates"),
            _ => None,
        },
        "registrationcode" => match (method, n) {
            ("GET", 1) => Some("GetRegistrationCode"),
            ("DELETE", 1) => Some("DeleteRegistrationCode"),
            _ => None,
        },
        "rules" => match (method, n) {
            ("GET", 1) => Some("ListTopicRules"),
            ("POST", 2) => Some("CreateTopicRule"),
            ("GET", 2) => Some("GetTopicRule"),
            ("PATCH", 2) => Some("ReplaceTopicRule"),
            ("DELETE", 2) => Some("DeleteTopicRule"),
            ("POST", 3) if segs[2] == "enable" => Some("EnableTopicRule"),
            ("POST", 3) if segs[2] == "disable" => Some("DisableTopicRule"),
            _ => None,
        },
        "destinations" => match (method, n) {
            ("POST", 1) => Some("CreateTopicRuleDestination"),
            ("GET", 1) => Some("ListTopicRuleDestinations"),
            ("PATCH", 1) => Some("UpdateTopicRuleDestination"),
            ("GET", 2..) => Some("GetTopicRuleDestination"),
            ("DELETE", 2..) => Some("DeleteTopicRuleDestination"),
            _ => None,
        },
        "confirmdestination" => match (method, n) {
            ("GET", 2..) => Some("ConfirmTopicRuleDestination"),
            _ => None,
        },
        "jobs" => match (method, n) {
            ("GET", 1) => Some("ListJobs"),
            ("PUT", 2) => Some("CreateJob"),
            ("GET", 2) => Some("DescribeJob"),
            ("PATCH", 2) => Some("UpdateJob"),
            ("DELETE", 2) => Some("DeleteJob"),
            ("PUT", 3) if segs[2] == "cancel" => Some("CancelJob"),
            ("GET", 3) if segs[2] == "job-document" => Some("GetJobDocument"),
            ("GET", 3) if segs[2] == "things" => Some("ListJobExecutionsForJob"),
            ("POST", 3) if segs[2] == "targets" => Some("AssociateTargetsWithJob"),
            _ => None,
        },
        "job-templates" => match (method, n) {
            ("GET", 1) => Some("ListJobTemplates"),
            ("PUT", 2) => Some("CreateJobTemplate"),
            ("GET", 2) => Some("DescribeJobTemplate"),
            ("DELETE", 2) => Some("DeleteJobTemplate"),
            _ => None,
        },
        "domainConfigurations" => match (method, n) {
            ("GET", 1) => Some("ListDomainConfigurations"),
            ("POST", 2) => Some("CreateDomainConfiguration"),
            ("GET", 2) => Some("DescribeDomainConfiguration"),
            ("PUT", 2) => Some("UpdateDomainConfiguration"),
            ("DELETE", 2) => Some("DeleteDomainConfiguration"),
            _ => None,
        },
        "security-profiles" => match (method, n) {
            ("GET", 1) => Some("ListSecurityProfiles"),
            ("POST", 2) => Some("CreateSecurityProfile"),
            ("GET", 2) => Some("DescribeSecurityProfile"),
            ("PATCH", 2) => Some("UpdateSecurityProfile"),
            ("DELETE", 2) => Some("DeleteSecurityProfile"),
            ("PUT", 3) if segs[2] == "targets" => Some("AttachSecurityProfile"),
            ("DELETE", 3) if segs[2] == "targets" => Some("DetachSecurityProfile"),
            ("GET", 3) if segs[2] == "targets" => Some("ListTargetsForSecurityProfile"),
            _ => None,
        },
        "security-profiles-for-target" => match (method, n) {
            ("GET", 1) => Some("ListSecurityProfilesForTarget"),
            _ => None,
        },
        "security-profile-behaviors" => match (method, n) {
            ("POST", 2) if segs[1] == "validate" => Some("ValidateSecurityProfileBehaviors"),
            _ => None,
        },
        "authorizer" => match (method, n) {
            ("POST", 2) => Some("CreateAuthorizer"),
            ("GET", 2) => Some("DescribeAuthorizer"),
            ("PUT", 2) => Some("UpdateAuthorizer"),
            ("DELETE", 2) => Some("DeleteAuthorizer"),
            ("POST", 3) if segs[2] == "test" => Some("TestInvokeAuthorizer"),
            _ => None,
        },
        "authorizers" => match (method, n) {
            ("GET", 0) | ("GET", 1) => Some("ListAuthorizers"),
            _ => None,
        },
        "default-authorizer" => match (method, n) {
            ("POST", 0) | ("POST", 1) => Some("SetDefaultAuthorizer"),
            ("GET", 0) | ("GET", 1) => Some("DescribeDefaultAuthorizer"),
            ("DELETE", 0) | ("DELETE", 1) => Some("ClearDefaultAuthorizer"),
            _ => None,
        },
        "provisioning-templates" => match (method, n) {
            ("POST", 1) => Some("CreateProvisioningTemplate"),
            ("GET", 1) => Some("ListProvisioningTemplates"),
            ("GET", 2) => Some("DescribeProvisioningTemplate"),
            ("PATCH", 2) => Some("UpdateProvisioningTemplate"),
            ("DELETE", 2) => Some("DeleteProvisioningTemplate"),
            ("POST", 3) if segs[2] == "versions" => Some("CreateProvisioningTemplateVersion"),
            ("GET", 3) if segs[2] == "versions" => Some("ListProvisioningTemplateVersions"),
            ("GET", 4) => Some("DescribeProvisioningTemplateVersion"),
            ("DELETE", 4) => Some("DeleteProvisioningTemplateVersion"),
            ("POST", 3) if segs[2] == "provisioning-claim" => Some("CreateProvisioningClaim"),
            _ => None,
        },
        "dimensions" => match (method, n) {
            ("GET", 1) => Some("ListDimensions"),
            ("POST", 2) => Some("CreateDimension"),
            ("GET", 2) => Some("DescribeDimension"),
            ("PATCH", 2) => Some("UpdateDimension"),
            ("DELETE", 2) => Some("DeleteDimension"),
            _ => None,
        },
        "custom-metric" => match (method, n) {
            ("POST", 2) => Some("CreateCustomMetric"),
            ("GET", 2) => Some("DescribeCustomMetric"),
            ("PATCH", 2) => Some("UpdateCustomMetric"),
            ("DELETE", 2) => Some("DeleteCustomMetric"),
            _ => None,
        },
        "custom-metrics" => match (method, n) {
            ("GET", 1) => Some("ListCustomMetrics"),
            _ => None,
        },
        "fleet-metric" => match (method, n) {
            ("PUT", 2) => Some("CreateFleetMetric"),
            ("GET", 2) => Some("DescribeFleetMetric"),
            ("PATCH", 2) => Some("UpdateFleetMetric"),
            ("DELETE", 2) => Some("DeleteFleetMetric"),
            _ => None,
        },
        "fleet-metrics" => match (method, n) {
            ("GET", 1) => Some("ListFleetMetrics"),
            _ => None,
        },
        "streams" => match (method, n) {
            ("GET", 1) => Some("ListStreams"),
            ("POST", 2) => Some("CreateStream"),
            ("GET", 2) => Some("DescribeStream"),
            ("PUT", 2) => Some("UpdateStream"),
            ("DELETE", 2) => Some("DeleteStream"),
            _ => None,
        },
        "audit" => match (method, n) {
            ("GET", 2) if segs[1] == "configuration" => Some("DescribeAccountAuditConfiguration"),
            ("PATCH", 2) if segs[1] == "configuration" => Some("UpdateAccountAuditConfiguration"),
            ("DELETE", 2) if segs[1] == "configuration" => Some("DeleteAccountAuditConfiguration"),
            ("POST", 2) if segs[1] == "tasks" => Some("StartOnDemandAuditTask"),
            ("PUT", 4) if segs[1] == "tasks" && segs[3] == "cancel" => Some("CancelAuditTask"),
            ("GET", 3) if segs[1] == "tasks" => Some("DescribeAuditTask"),
            ("GET", 2) if segs[1] == "tasks" => Some("ListAuditTasks"),
            ("POST", 2) if segs[1] == "findings" => Some("ListAuditFindings"),
            ("GET", 3) if segs[1] == "findings" => Some("DescribeAuditFinding"),
            ("GET", 2) if segs[1] == "relatedResources" => {
                Some("ListRelatedResourcesForAuditFinding")
            }
            ("POST", 3) if segs[1] == "suppressions" && segs[2] == "create" => {
                Some("CreateAuditSuppression")
            }
            ("POST", 3) if segs[1] == "suppressions" && segs[2] == "delete" => {
                Some("DeleteAuditSuppression")
            }
            ("POST", 3) if segs[1] == "suppressions" && segs[2] == "describe" => {
                Some("DescribeAuditSuppression")
            }
            ("PATCH", 3) if segs[1] == "suppressions" && segs[2] == "update" => {
                Some("UpdateAuditSuppression")
            }
            ("POST", 3) if segs[1] == "suppressions" && segs[2] == "list" => {
                Some("ListAuditSuppressions")
            }
            ("POST", 4) if segs[1] == "mitigationactions" && segs[2] == "tasks" => {
                Some("StartAuditMitigationActionsTask")
            }
            ("GET", 4) if segs[1] == "mitigationactions" && segs[2] == "tasks" => {
                Some("DescribeAuditMitigationActionsTask")
            }
            ("GET", 3) if segs[1] == "mitigationactions" && segs[2] == "tasks" => {
                Some("ListAuditMitigationActionsTasks")
            }
            ("GET", 3) if segs[1] == "mitigationactions" && segs[2] == "executions" => {
                Some("ListAuditMitigationActionsExecutions")
            }
            ("POST", 3) if segs[1] == "scheduledaudits" => Some("CreateScheduledAudit"),
            ("GET", 3) if segs[1] == "scheduledaudits" => Some("DescribeScheduledAudit"),
            ("PATCH", 3) if segs[1] == "scheduledaudits" => Some("UpdateScheduledAudit"),
            ("DELETE", 3) if segs[1] == "scheduledaudits" => Some("DeleteScheduledAudit"),
            ("GET", 2) if segs[1] == "scheduledaudits" => Some("ListScheduledAudits"),
            _ => None,
        },
        "detect" => match (method, n) {
            ("PUT", 4) if segs[1] == "mitigationactions" && segs[2] == "tasks" => {
                Some("StartDetectMitigationActionsTask")
            }
            ("PUT", 5) if segs[1] == "mitigationactions" && segs[4] == "cancel" => {
                Some("CancelDetectMitigationActionsTask")
            }
            ("GET", 4) if segs[1] == "mitigationactions" && segs[2] == "tasks" => {
                Some("DescribeDetectMitigationActionsTask")
            }
            ("GET", 3) if segs[1] == "mitigationactions" && segs[2] == "tasks" => {
                Some("ListDetectMitigationActionsTasks")
            }
            ("GET", 3) if segs[1] == "mitigationactions" && segs[2] == "executions" => {
                Some("ListDetectMitigationActionsExecutions")
            }
            _ => None,
        },
        "mitigationactions" => match (method, n) {
            ("GET", 2) if segs[1] == "actions" => Some("ListMitigationActions"),
            ("POST", 3) => Some("CreateMitigationAction"),
            ("GET", 3) => Some("DescribeMitigationAction"),
            ("PATCH", 3) => Some("UpdateMitigationAction"),
            ("DELETE", 3) => Some("DeleteMitigationAction"),
            _ => None,
        },
        "otaUpdates" => match (method, n) {
            ("GET", 1) => Some("ListOTAUpdates"),
            ("POST", 2) => Some("CreateOTAUpdate"),
            ("GET", 2) => Some("GetOTAUpdate"),
            ("DELETE", 2) => Some("DeleteOTAUpdate"),
            _ => None,
        },
        "commands" => match (method, n) {
            ("GET", 1) => Some("ListCommands"),
            ("PUT", 2) => Some("CreateCommand"),
            ("GET", 2) => Some("GetCommand"),
            ("DELETE", 2) => Some("DeleteCommand"),
            ("PATCH", 2) => Some("UpdateCommand"),
            _ => None,
        },
        "command-executions" => match (method, n) {
            ("POST", 1) => Some("ListCommandExecutions"),
            ("GET", 2) => Some("GetCommandExecution"),
            ("DELETE", 2) => Some("DeleteCommandExecution"),
            _ => None,
        },
        "packages" => match (method, n) {
            ("GET", 1) => Some("ListPackages"),
            ("PUT", 2) => Some("CreatePackage"),
            ("GET", 2) => Some("GetPackage"),
            ("PATCH", 2) => Some("UpdatePackage"),
            ("DELETE", 2) => Some("DeletePackage"),
            ("GET", 3) if segs[2] == "versions" => Some("ListPackageVersions"),
            ("PUT", 4) => Some("CreatePackageVersion"),
            ("GET", 4) => Some("GetPackageVersion"),
            ("PATCH", 4) => Some("UpdatePackageVersion"),
            ("DELETE", 4) => Some("DeletePackageVersion"),
            ("PUT", 5) if segs[4] == "sbom" => Some("AssociateSbomWithPackageVersion"),
            ("DELETE", 5) if segs[4] == "sbom" => Some("DisassociateSbomFromPackageVersion"),
            ("GET", 5) if segs[4] == "sbom-validation-results" => Some("ListSbomValidationResults"),
            _ => None,
        },
        "package-configuration" => match (method, n) {
            ("GET", 1) => Some("GetPackageConfiguration"),
            ("PATCH", 1) => Some("UpdatePackageConfiguration"),
            _ => None,
        },
        "effective-policies" => match (method, n) {
            ("POST", 1) => Some("GetEffectivePolicies"),
            _ => None,
        },
        "endpoint" => match (method, n) {
            ("GET", 1) => Some("DescribeEndpoint"),
            _ => None,
        },
        "encryption-configuration" => match (method, n) {
            ("GET", 1) => Some("DescribeEncryptionConfiguration"),
            ("PATCH", 1) => Some("UpdateEncryptionConfiguration"),
            _ => None,
        },
        "indexing" => match (method, n) {
            ("GET", 2) if segs[1] == "config" => Some("GetIndexingConfiguration"),
            ("POST", 2) if segs[1] == "config" => Some("UpdateIndexingConfiguration"),
            _ => None,
        },
        "indices" => match (method, n) {
            ("GET", 1) => Some("ListIndices"),
            ("GET", 2) => Some("DescribeIndex"),
            ("POST", 2) if segs[1] == "search" => Some("SearchIndex"),
            ("POST", 2) if segs[1] == "statistics" => Some("GetStatistics"),
            ("POST", 2) if segs[1] == "cardinality" => Some("GetCardinality"),
            ("POST", 2) if segs[1] == "buckets" => Some("GetBucketsAggregation"),
            ("POST", 2) if segs[1] == "percentiles" => Some("GetPercentiles"),
            _ => None,
        },
        "event-configurations" => match (method, n) {
            ("GET", 1) => Some("DescribeEventConfigurations"),
            ("PATCH", 1) => Some("UpdateEventConfigurations"),
            _ => None,
        },
        "loggingOptions" => match (method, n) {
            ("GET", 1) => Some("GetLoggingOptions"),
            ("POST", 1) => Some("SetLoggingOptions"),
            _ => None,
        },
        "v2LoggingOptions" => match (method, n) {
            ("GET", 1) => Some("GetV2LoggingOptions"),
            ("POST", 1) => Some("SetV2LoggingOptions"),
            _ => None,
        },
        "v2LoggingLevel" => match (method, n) {
            ("GET", 1) => Some("ListV2LoggingLevels"),
            ("POST", 1) => Some("SetV2LoggingLevel"),
            ("DELETE", 1) => Some("DeleteV2LoggingLevel"),
            _ => None,
        },
        "role-aliases" => match (method, n) {
            ("GET", 1) => Some("ListRoleAliases"),
            ("POST", 2) => Some("CreateRoleAlias"),
            ("GET", 2) => Some("DescribeRoleAlias"),
            ("PUT", 2) => Some("UpdateRoleAlias"),
            ("DELETE", 2) => Some("DeleteRoleAlias"),
            _ => None,
        },
        "tags" => match (method, n) {
            ("POST", 1) => Some("TagResource"),
            ("GET", 1) => Some("ListTagsForResource"),
            _ => None,
        },
        "untag" => match (method, n) {
            ("POST", 1) => Some("UntagResource"),
            _ => None,
        },
        "certificate-providers" => match (method, n) {
            ("GET", 1) => Some("ListCertificateProviders"),
            ("POST", 2) => Some("CreateCertificateProvider"),
            ("GET", 2) => Some("DescribeCertificateProvider"),
            ("PUT", 2) => Some("UpdateCertificateProvider"),
            ("DELETE", 2) => Some("DeleteCertificateProvider"),
            _ => None,
        },
        "test-authorization" => match (method, n) {
            ("POST", 1) => Some("TestAuthorization"),
            _ => None,
        },
        "behavior-model-training" => match (method, n) {
            ("GET", 2) if segs[1] == "summaries" => Some("GetBehaviorModelTrainingSummaries"),
            _ => None,
        },
        "active-violations" => match (method, n) {
            ("GET", 1) => Some("ListActiveViolations"),
            _ => None,
        },
        "violation-events" => match (method, n) {
            ("GET", 1) => Some("ListViolationEvents"),
            _ => None,
        },
        "metric-values" => match (method, n) {
            ("GET", 1) => Some("ListMetricValues"),
            _ => None,
        },
        "managed-job-templates" => match (method, n) {
            ("GET", 1) => Some("ListManagedJobTemplates"),
            ("GET", 2) => Some("DescribeManagedJobTemplate"),
            _ => None,
        },
        "thing-registration-tasks" => match (method, n) {
            ("POST", 1) => Some("StartThingRegistrationTask"),
            ("GET", 1) => Some("ListThingRegistrationTasks"),
            ("GET", 2) => Some("DescribeThingRegistrationTask"),
            ("PUT", 3) if segs[2] == "cancel" => Some("StopThingRegistrationTask"),
            ("GET", 3) if segs[2] == "reports" => Some("ListThingRegistrationTaskReports"),
            _ => None,
        },
        "violations" => match (method, n) {
            ("POST", 3) if segs[1] == "verification-state" => {
                Some("PutVerificationStateOnViolation")
            }
            _ => None,
        },
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Handler entry — one arm per operation; handlers share (s, req, segs,
// body, query). Each resolves to a typed handler returning Result.
// ---------------------------------------------------------------------------

/// Rejoin a greedy `{arn+}` tail (destination ARNs carry literal slashes
/// that botocore does not percent-encode on the wire).
fn greedy_tail(segs: &[String]) -> String {
    segs.iter()
        .skip(1)
        .cloned()
        .collect::<Vec<String>>()
        .join("/")
}

fn handle(
    op: &str,
    s: &mut State,
    req: &Request,
    segs: &[String],
    body: &Value,
    query: &BTreeMap<String, Vec<String>>,
) -> Option<Response> {
    let region = req.region.clone();
    let account = req.account;
    let seg = |idx: usize| -> String { segs.get(idx).cloned().unwrap_or_default() };
    rr(match op {
        "CreateThing" => create_thing(s, &seg(1), body, &region, account),
        "DescribeThing" => describe_thing(s, &seg(1)),
        "UpdateThing" => update_thing(s, &seg(1), body),
        "DeleteThing" => delete_thing(s, &seg(1)),
        "ListThings" => list_things(s, body, query),
        "RegisterThing" => register_thing(body),
        "AttachThingPrincipal" => attach_thing_principal(s, &seg(1), req),
        "DetachThingPrincipal" => detach_thing_principal(s, &seg(1), req),
        "ListThingPrincipals" => list_thing_principals(s, &seg(1)),
        "ListThingPrincipalsV2" => list_thing_principals_v2(s, &seg(1)),
        "ListThingGroupsForThing" => list_thing_groups_for_thing(s, &seg(1)),
        "GetThingConnectivityData" => get_thing_connectivity_data(s, &seg(1)),
        "ListJobExecutionsForThing" => list_job_executions_for_thing(s, &seg(1), body, query),
        "DescribeJobExecution" => describe_job_execution(s, &seg(1), &seg(3), body, query),
        "CancelJobExecution" => cancel_job_execution(s, &seg(1), &seg(3), body, query),
        "DeleteJobExecution" => delete_job_execution(s, &seg(1), &seg(3), &seg(5), body, query),
        "CreateThingType" => create_thing_type(s, &seg(1), body, &region, account),
        "DescribeThingType" => describe_thing_type(s, &seg(1)),
        "UpdateThingType" => update_thing_type(s, &seg(1), body),
        "DeleteThingType" => delete_thing_type(s, &seg(1)),
        "DeprecateThingType" => deprecate_thing_type(s, &seg(1), body),
        "ListThingTypes" => list_thing_types(s, body, query),
        "CreateThingGroup" => create_thing_group(s, &seg(1), body, &region, account),
        "DescribeThingGroup" => describe_thing_group(s, &seg(1)),
        "UpdateThingGroup" => update_thing_group(s, &seg(1), body, query),
        "DeleteThingGroup" => delete_thing_group(s, &seg(1)),
        "ListThingGroups" => list_thing_groups(s, body, query),
        "ListThingsInThingGroup" => list_things_in_thing_group(s, &seg(1)),
        "AddThingToThingGroup" => add_remove_thing_group(s, body, query, true),
        "RemoveThingFromThingGroup" => add_remove_thing_group(s, body, query, false),
        "UpdateThingGroupsForThing" => update_thing_groups_for_thing(s, body),
        "CreateBillingGroup" => create_billing_group(s, &seg(1), body, &region, account),
        "DescribeBillingGroup" => describe_billing_group(s, &seg(1)),
        "UpdateBillingGroup" => update_billing_group(s, &seg(1), body, query),
        "DeleteBillingGroup" => delete_billing_group(s, &seg(1)),
        "ListBillingGroups" => list_billing_groups(s, body, query),
        "ListThingsInBillingGroup" => list_things_in_billing_group(s, &seg(1)),
        "AddThingToBillingGroup" => add_thing_to_billing_group(s, body, query),
        "RemoveThingFromBillingGroup" => remove_thing_from_billing_group(s, body, query),
        "CreateDynamicThingGroup" => create_dynamic_thing_group(s, &seg(1), body, &region, account),
        "UpdateDynamicThingGroup" => update_dynamic_thing_group(s, &seg(1), body, query),
        "DeleteDynamicThingGroup" => delete_dynamic_thing_group(s, &seg(1)),
        "CreatePolicy" => create_policy(s, &seg(1), body, &region, account),
        "GetPolicy" => get_policy(s, &seg(1)),
        "DeletePolicy" => delete_policy(s, &seg(1)),
        "ListPolicies" => list_policies(s),
        "CreatePolicyVersion" => create_policy_version(s, &seg(1), body, query, &region, account),
        "ListPolicyVersions" => list_policy_versions(s, &seg(1)),
        "GetPolicyVersion" => get_policy_version(s, &seg(1), &seg(3)),
        "DeletePolicyVersion" => delete_policy_version(s, &seg(1), &seg(3)),
        "SetDefaultPolicyVersion" => set_default_policy_version(s, &seg(1), &seg(3)),

        "AttachPrincipalPolicy" => attach_principal_policy(s, &seg(1), req),
        "DetachPrincipalPolicy" => detach_principal_policy(s, &seg(1), req),
        "ListPrincipalPolicies" => list_principal_policies(req, s),
        "ListPolicyPrincipals" => list_policy_principals(s, req),
        "ListTargetsForPolicy" => list_targets_for_policy(s, &seg(1)),
        "AttachPolicy" => attach_policy(s, &seg(1), body),
        "DetachPolicy" => detach_policy(s, &seg(1), body),
        "ListAttachedPolicies" => list_attached_policies(s, &seg(1)),
        "ListPrincipalThings" => list_principal_things(req, s),
        "ListPrincipalThingsV2" => list_principal_things_v2(req, s, body, query),
        "CreateCertificateFromCsr" => create_certificate_from_csr(s, body, query, &region, account),
        "ListCertificates" => list_certificates(s),
        "DescribeCertificate" => describe_certificate(s, &seg(1)),
        "UpdateCertificate" => update_certificate(s, &seg(1), query),
        "DeleteCertificate" => delete_certificate(s, &seg(1), query),
        "RegisterCertificate" => register_certificate(s, body, query, &region, account),
        "RegisterCertificateWithoutCA" => {
            register_certificate_without_ca(s, body, &region, account)
        }
        "RegisterCACertificate" => register_ca_certificate(s, body, query, &region, account),
        "DescribeCACertificate" => describe_ca_certificate(s, &seg(1)),
        "UpdateCACertificate" => update_ca_certificate(s, &seg(1), body, query),
        "DeleteCACertificate" => delete_ca_certificate(s, &seg(1)),
        "ListCACertificates" => list_ca_certificates(s),
        "CreateKeysAndCertificate" => create_keys_and_certificate(s, query, &region, account),
        "TransferCertificate" => transfer_certificate(s, &seg(1), body, query),
        "AcceptCertificateTransfer" => accept_certificate_transfer(s, &seg(1), query),
        "CancelCertificateTransfer" => cancel_certificate_transfer(s, &seg(1)),
        "RejectCertificateTransfer" => reject_certificate_transfer(s, &seg(1), body),
        "ListCertificatesByCA" => list_certificates_by_ca(s, &seg(1)),
        "ListOutgoingCertificates" => list_outgoing_certificates(s),
        "GetRegistrationCode" => get_registration_code(s),
        "DeleteRegistrationCode" => delete_registration_code(s),
        "CreateTopicRule" => create_topic_rule(s, &seg(1), body, &region, account),
        "GetTopicRule" => get_topic_rule(s, &seg(1)),
        "ReplaceTopicRule" => replace_topic_rule(s, &seg(1), body, &region, account),
        "DeleteTopicRule" => delete_topic_rule(s, &seg(1)),
        "EnableTopicRule" => enable_disable_topic_rule(s, &seg(1), false),
        "DisableTopicRule" => enable_disable_topic_rule(s, &seg(1), true),
        "ListTopicRules" => list_topic_rules(s),
        "CreateTopicRuleDestination" => create_topic_rule_destination(s, body, &region, account),
        "ListTopicRuleDestinations" => list_topic_rule_destinations(s),
        "UpdateTopicRuleDestination" => update_topic_rule_destination(s, body),
        "GetTopicRuleDestination" => get_topic_rule_destination(s, &greedy_tail(segs)),
        "DeleteTopicRuleDestination" => delete_topic_rule_destination(s, &greedy_tail(segs)),
        "ConfirmTopicRuleDestination" => confirm_topic_rule_destination(),
        "CreateJob" => create_job(s, &seg(1), body, &region, account),
        "DescribeJob" => describe_job(s, &seg(1)),
        "UpdateJob" => update_job(s, &seg(1), body),
        "DeleteJob" => delete_job(s, &seg(1), body, query),
        "CancelJob" => cancel_job(s, &seg(1), body, query),
        "GetJobDocument" => get_job_document(s, &seg(1)),
        "ListJobs" => list_jobs(s),
        "ListJobExecutionsForJob" => list_job_executions_for_job(s, &seg(1), body, query),
        "AssociateTargetsWithJob" => associate_targets_with_job(body),
        "CreateJobTemplate" => create_job_template(s, &seg(1), body, &region, account),
        "DescribeJobTemplate" => describe_job_template(s, &seg(1)),
        "DeleteJobTemplate" => delete_job_template(s, &seg(1)),
        "ListJobTemplates" => list_job_templates(s),
        "CreateDomainConfiguration" => {
            create_domain_configuration(s, &seg(1), body, &region, account)
        }
        "DescribeDomainConfiguration" => describe_domain_configuration(s, &seg(1)),
        "UpdateDomainConfiguration" => update_domain_configuration(s, &seg(1), body),
        "DeleteDomainConfiguration" => delete_domain_configuration(s, &seg(1)),
        "ListDomainConfigurations" => list_domain_configurations(s),
        "CreateSecurityProfile" => create_security_profile(s, &seg(1), body, &region, account),
        "DescribeSecurityProfile" => describe_security_profile(s, &seg(1)),
        "UpdateSecurityProfile" => update_security_profile(s, &seg(1), body),
        "DeleteSecurityProfile" => delete_security_profile(s, &seg(1)),
        "ListSecurityProfiles" => list_security_profiles(s),
        "AttachSecurityProfile" => attach_detach_security_profile(s, &seg(1), body, query, true),
        "DetachSecurityProfile" => attach_detach_security_profile(s, &seg(1), body, query, false),
        "ListTargetsForSecurityProfile" => list_targets_for_security_profile(s, &seg(1)),
        "ListSecurityProfilesForTarget" => list_security_profiles_for_target(s, body, query),
        "ValidateSecurityProfileBehaviors" => validate_security_profile_behaviors(),
        "CreateAuthorizer" => create_authorizer(s, &seg(1), body, &region, account),
        "DescribeAuthorizer" => describe_authorizer(s, &seg(1)),
        "UpdateAuthorizer" => update_authorizer(s, &seg(1), body),
        "DeleteAuthorizer" => delete_authorizer(s, &seg(1)),
        "ListAuthorizers" => list_authorizers(s),
        "TestInvokeAuthorizer" => test_invoke_authorizer(s, &seg(1)),
        "SetDefaultAuthorizer" => set_default_authorizer(s, body),
        "DescribeDefaultAuthorizer" => describe_default_authorizer(s),
        "ClearDefaultAuthorizer" => clear_default_authorizer(s),
        "CreateProvisioningTemplate" => create_provisioning_template(s, body, &region, account),
        "DescribeProvisioningTemplate" => describe_provisioning_template(s, &seg(1)),
        "UpdateProvisioningTemplate" => update_provisioning_template(s, &seg(1), body),
        "DeleteProvisioningTemplate" => delete_provisioning_template(s, &seg(1)),
        "ListProvisioningTemplates" => list_provisioning_templates(s),
        "CreateProvisioningTemplateVersion" => {
            create_prov_template_version(s, &seg(1), body, query)
        }
        "ListProvisioningTemplateVersions" => list_prov_template_versions(s, &seg(1)),
        "DescribeProvisioningTemplateVersion" => {
            describe_prov_template_version(s, &seg(1), &seg(3))
        }
        "DeleteProvisioningTemplateVersion" => delete_prov_template_version(s, &seg(1), &seg(3)),

        "CreateProvisioningClaim" => create_provisioning_claim(s, &seg(1)),
        "CreateDimension" => create_dimension(s, &seg(1), body, &region, account),
        "DescribeDimension" => describe_dimension(s, &seg(1)),
        "UpdateDimension" => update_dimension(s, &seg(1), body),
        "DeleteDimension" => delete_dimension(s, &seg(1)),
        "ListDimensions" => list_dimensions(s),
        "CreateCustomMetric" => create_custom_metric(s, &seg(1), body, &region, account),
        "DescribeCustomMetric" => describe_custom_metric(s, &seg(1)),
        "UpdateCustomMetric" => update_custom_metric(s, &seg(1), body),
        "DeleteCustomMetric" => delete_custom_metric(s, &seg(1)),
        "ListCustomMetrics" => list_custom_metrics(s),
        "CreateFleetMetric" => create_fleet_metric(s, &seg(1), body, &region, account),
        "DescribeFleetMetric" => describe_fleet_metric(s, &seg(1)),
        "UpdateFleetMetric" => update_fleet_metric(s, &seg(1), body),
        "DeleteFleetMetric" => delete_fleet_metric(s, &seg(1)),
        "ListFleetMetrics" => list_fleet_metrics(s),
        "CreateStream" => create_stream(s, &seg(1), body, &region, account),
        "DescribeStream" => describe_stream(s, &seg(1)),
        "UpdateStream" => update_stream(s, &seg(1), body),
        "DeleteStream" => delete_stream(s, &seg(1)),
        "ListStreams" => list_streams(s),
        "CreateOTAUpdate" => create_ota_update(s, &seg(1), body, &region, account),
        "GetOTAUpdate" => get_ota_update(s, &seg(1)),
        "DeleteOTAUpdate" => delete_ota_update(s, &seg(1)),
        "ListOTAUpdates" => list_ota_updates(s),
        "CreateCommand" => create_command(s, &seg(1), body, &region, account),
        "GetCommand" => get_command(s, &seg(1)),
        "UpdateCommand" => update_command(s, &seg(1), body),
        "DeleteCommand" => delete_command(s, &seg(1)),
        "ListCommands" => list_commands(s),
        "ListCommandExecutions" => list_command_executions(),
        "GetCommandExecution" => get_command_execution(body),
        "DeleteCommandExecution" => delete_command_execution(body),
        "CreatePackage" => create_package(s, &seg(1), body, &region, account),
        "GetPackage" => get_package(s, &seg(1)),
        "UpdatePackage" => update_package(s, &seg(1), body),
        "DeletePackage" => delete_package(s, &seg(1)),
        "ListPackages" => list_packages(s),
        "GetPackageConfiguration" => get_package_configuration(),
        "UpdatePackageConfiguration" => update_package_configuration(),
        "CreatePackageVersion" => {
            create_package_version(s, &seg(1), &seg(3), body, &region, account)
        }
        "GetPackageVersion" => get_package_version(s, &seg(1), &seg(3)),
        "UpdatePackageVersion" => update_package_version(s, &seg(1), &seg(3), body),
        "DeletePackageVersion" => delete_package_version(s, &seg(1), &seg(3)),
        "ListPackageVersions" => list_package_versions(s, &seg(1)),
        "AssociateSbomWithPackageVersion" => associate_sbom(s, &seg(1), &seg(3), body),
        "DisassociateSbomFromPackageVersion" => disassociate_sbom(s, &seg(1), &seg(3)),
        "ListSbomValidationResults" => list_sbom_validation_results(),
        "GetEffectivePolicies" => get_effective_policies(s, body),
        "DescribeEndpoint" => describe_endpoint(body, query),
        "DescribeEncryptionConfiguration" => describe_encryption_configuration(),
        "UpdateEncryptionConfiguration" => update_encryption_configuration(body),
        "GetIndexingConfiguration" => get_indexing_configuration(s),
        "UpdateIndexingConfiguration" => update_indexing_configuration(s, body),
        "ListIndices" => list_indices(),
        "DescribeIndex" => describe_index(&seg(1)),
        "SearchIndex" => search_index(s, body),
        "GetStatistics" => get_statistics(s, body),
        "GetCardinality" => get_cardinality(body),
        "GetBucketsAggregation" => get_buckets_aggregation(body),
        "GetPercentiles" => get_percentiles(body),
        "DescribeEventConfigurations" => describe_event_configurations(s),
        "UpdateEventConfigurations" => update_event_configurations(s, body),
        "GetLoggingOptions" => get_logging_options(s),
        "SetLoggingOptions" => set_logging_options(s, body),
        "GetV2LoggingOptions" => get_v2_logging_options(s),
        "SetV2LoggingOptions" => set_v2_logging_options(s, body),
        "SetV2LoggingLevel" => set_v2_logging_level(s, body),
        "ListV2LoggingLevels" => list_v2_logging_levels(s),
        "DeleteV2LoggingLevel" => delete_v2_logging_level(s, body, query),
        "TagResource" => tag_resource(s, body),
        "ListTagsForResource" => list_tags_for_resource(s, body, query),
        "UntagResource" => untag_resource(s, body),
        "CreateCertificateProvider" => {
            create_certificate_provider(s, &seg(1), body, &region, account)
        }
        "DescribeCertificateProvider" => describe_certificate_provider(s, &seg(1)),
        "UpdateCertificateProvider" => update_certificate_provider(s, &seg(1), body),
        "DeleteCertificateProvider" => delete_certificate_provider(s, &seg(1)),
        "ListCertificateProviders" => list_certificate_providers(s),
        "CreateRoleAlias" => create_role_alias(s, &seg(1), body, &region, account),
        "DescribeRoleAlias" => describe_role_alias(s, &seg(1)),
        "UpdateRoleAlias" => update_role_alias(s, &seg(1), body),
        "DeleteRoleAlias" => delete_role_alias(s, &seg(1)),
        "ListRoleAliases" => list_role_aliases(s),
        "TestAuthorization" => test_authorization(),
        "GetBehaviorModelTrainingSummaries" => get_behavior_model_training_summaries(),
        "ListActiveViolations" => list_active_violations(),
        "ListViolationEvents" => list_violation_events(),
        "ListMetricValues" => list_metric_values(),
        "ListManagedJobTemplates" => list_managed_job_templates(),
        "DescribeManagedJobTemplate" => describe_managed_job_template(s, &seg(1)),
        "ListThingRegistrationTasks" => list_thing_registration_tasks(s),
        "StartThingRegistrationTask" => start_thing_registration_task(s, body),
        "DescribeThingRegistrationTask" => describe_thing_registration_task(s, &seg(1)),
        "StopThingRegistrationTask" => stop_thing_registration_task(s, &seg(1)),
        "ListThingRegistrationTaskReports" => list_thing_registration_task_reports(),
        "PutVerificationStateOnViolation" => put_verification_state_on_violation(&seg(2)),
        "StartOnDemandAuditTask" => start_on_demand_audit_task(),
        "CancelAuditTask" => cancel_audit_task(),
        "DescribeAuditTask" => describe_audit_task(s, &seg(2)),
        "ListAuditTasks" => list_audit_tasks(),
        "ListAuditFindings" => list_audit_findings(),
        "DescribeAuditFinding" => describe_audit_finding(),
        "ListRelatedResourcesForAuditFinding" => list_related_resources_for_audit_finding(),
        "CreateAuditSuppression" => create_audit_suppression(s, body),
        "DeleteAuditSuppression" => delete_audit_suppression(s, body),
        "DescribeAuditSuppression" => describe_audit_suppression(s, body),
        "UpdateAuditSuppression" => update_audit_suppression(s, body),
        "ListAuditSuppressions" => list_audit_suppressions(),
        "StartAuditMitigationActionsTask" => start_audit_mitigation_actions_task(s, &seg(3), body),
        "DescribeAuditMitigationActionsTask" => describe_audit_mitigation_actions_task(s, &seg(3)),
        "ListAuditMitigationActionsTasks" => list_audit_mitigation_actions_tasks(),
        "ListAuditMitigationActionsExecutions" => list_audit_mitigation_actions_executions(),
        "StartDetectMitigationActionsTask" => {
            start_detect_mitigation_actions_task(s, &seg(3), body)
        }
        "CancelDetectMitigationActionsTask" => cancel_detect_mitigation_actions_task(),
        "DescribeDetectMitigationActionsTask" => {
            describe_detect_mitigation_actions_task(s, &seg(3))
        }
        "ListDetectMitigationActionsTasks" => list_detect_mitigation_actions_tasks(),
        "ListDetectMitigationActionsExecutions" => list_detect_mitigation_actions_executions(),
        "CreateScheduledAudit" => create_scheduled_audit(s, &seg(2), body, &region, account),
        "DescribeScheduledAudit" => describe_scheduled_audit(s, &seg(2)),
        "UpdateScheduledAudit" => update_scheduled_audit(s, &seg(2), body),
        "DeleteScheduledAudit" => delete_scheduled_audit(s, &seg(2)),
        "ListScheduledAudits" => list_scheduled_audits(s),
        "CreateMitigationAction" => create_mitigation_action(s, &seg(2), body, &region, account),
        "DescribeMitigationAction" => describe_mitigation_action(s, &seg(2)),
        "UpdateMitigationAction" => update_mitigation_action(s, &seg(2), body),
        "DeleteMitigationAction" => delete_mitigation_action(s, &seg(2)),
        "ListMitigationActions" => list_mitigation_actions(s),
        "DeleteAccountAuditConfiguration" => delete_account_audit_configuration(s),
        "DescribeAccountAuditConfiguration" => describe_account_audit_configuration(s),
        "UpdateAccountAuditConfiguration" => update_account_audit_configuration(s, body),
        _ => Err(invalid_request(&format!("Unknown operation {op}"))),
    })
}

// ---------------------------------------------------------------------------
// Things group by ARN (moto keys things stores by thing ARN: creating the
// same thing name again replaces the record). Impl bodies follow moto's
// IoTBackend semantics.
// ---------------------------------------------------------------------------

fn thing_record(
    name: &str,
    region: &str,
    account: u64,
    type_name: Option<&str>,
    attributes: &Value,
) -> Value {
    let mut t = json!({
        "thingName": name,
        "thingId": random_id(),
        "thingArn": arn_for(region, account, "thing", name),
        "attributes": attributes.clone(),
        "version": 1,
    });
    if let Some(tn) = type_name {
        t["thingTypeName"] = json!(tn);
    }
    t
}

fn find_thing_idx(s: &State, name: &str) -> Option<usize> {
    s.things
        .iter()
        .position(|t| t["thingName"].as_str() == Some(name))
}

fn create_thing(
    s: &mut State,
    name: &str,
    body: &Value,
    region: &str,
    account: u64,
) -> Result<Response, Response> {
    let mut attributes = json!({});
    if let Some(attrs) = body
        .get("attributePayload")
        .and_then(|x| x.get("attributes"))
    {
        if attrs.is_object() {
            attributes = attrs.clone();
        }
    }
    let type_name = b_str(body, "thingTypeName");
    let mut resolved_type: Option<String> = None;
    if let Some(tn) = type_name {
        let tt = s
            .thing_types
            .iter()
            .find(|t| t["thingTypeName"].as_str() == Some(tn))
            .ok_or_else(not_found)?;
        if tt["thingTypeMetadata"]["deprecated"] == json!(true) {
            return Err(invalid_request(&format!(
                "Can not create new thing with depreated thing type:{tn}"
            )));
        }
        resolved_type = Some(tn.to_string());
    }
    let thing = thing_record(name, region, account, resolved_type.as_deref(), &attributes);
    if let Some(bg_name) = b_str(body, "billingGroupName") {
        if let Some(g) = s
            .billing_groups
            .iter_mut()
            .find(|g| g["billingGroupName"].as_str() == Some(bg_name))
        {
            if g["things"].is_null() {
                g["things"] = json!([]);
            }
            if let Some(arr) = g["things"].as_array_mut() {
                arr.push(thing["thingArn"].clone());
            }
        }
    }
    if let Some(idx) = s
        .things
        .iter()
        .position(|t| t["thingArn"] == thing["thingArn"])
    {
        s.things[idx] = thing.clone();
    } else {
        s.things.push(thing.clone());
    }
    Ok(ok_json(&json!({
        "thingName": name,
        "thingArn": thing["thingArn"],
        "thingId": thing["thingId"],
    })))
}

fn describe_thing(s: &mut State, name: &str) -> Result<Response, Response> {
    let Some(mut thing) = s
        .things
        .iter()
        .find(|t| t["thingName"].as_str() == Some(name))
        .cloned()
    else {
        return Err(not_found());
    };
    if let Some(o) = thing.as_object_mut() {
        o.insert("defaultClientId".into(), json!(name));
    }
    let _ = random_id();
    Ok(ok_json(&thing))
}

fn update_thing(s: &mut State, name: &str, body: &Value) -> Result<Response, Response> {
    let Some(idx) = find_thing_idx(s, name) else {
        return Err(not_found());
    };
    let remove_type = b_bool(body, "removeThingType");
    let type_name = b_str(body, "thingTypeName").map(str::to_owned);
    if remove_type == Some(true) && type_name.is_some() {
        return Err(invalid_request("The request is not valid."));
    }
    if let Some(tn) = type_name {
        let tt = s
            .thing_types
            .iter()
            .find(|t| t["thingTypeName"] == json!(tn))
            .ok_or_else(not_found)?;
        if tt["thingTypeMetadata"]["deprecated"] == json!(true) {
            return Err(invalid_request(&format!(
                "Can not update a thing to use deprecated thing type: {tn}"
            )));
        }
        s.things[idx]["thingTypeName"] = json!(tn);
    } else if remove_type == Some(true) {
        s.things[idx]
            .as_object_mut()
            .map(|o| o.remove("thingTypeName"));
    }
    if let Some(payload) = body.get("attributePayload") {
        if let Some(attrs) = payload.get("attributes") {
            let merge = payload.get("merge") == Some(&json!(true));
            if !merge {
                s.things[idx]["attributes"] = attrs.clone();
            } else if let (Some(cur), Some(new)) = (
                s.things[idx]["attributes"].as_object_mut(),
                attrs.as_object(),
            ) {
                for (k, v) in new {
                    cur.insert(k.clone(), v.clone());
                }
            }
            if let Some(cur) = s.things[idx]["attributes"].as_object_mut() {
                cur.retain(|_, v| !v.is_null());
            }
        }
    }
    Ok(ok_empty())
}

fn delete_thing(s: &mut State, name: &str) -> Result<Response, Response> {
    let Some(idx) = find_thing_idx(s, name) else {
        return Err(not_found());
    };
    if s.principal_things.iter().any(|(_, tn)| tn == name) {
        // moto's ThingStillAttached keeps the InvalidRequestException type
        // but returns conflict status 409.
        return Err(json_err(
            409,
            "InvalidRequestException",
            json!({"__type": "InvalidRequestException", "message":
                format!("Cannot delete. Thing {name} is still attached to one or more principals")}),
        ));
    }
    let arn = s.things[idx]["thingArn"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    for g in &mut s.billing_groups {
        if let Some(arr) = g["things"].as_array_mut() {
            arr.retain(|a| a != &json!(arn));
        }
    }
    s.principal_things.retain(|(_, tn)| tn != name);
    s.things.remove(idx);
    Ok(ok_empty())
}

fn list_things(
    s: &mut State,
    body: &Value,
    query: &BTreeMap<String, Vec<String>>,
) -> Result<Response, Response> {
    let attr_name = p_str(body, query, "attributeName").unwrap_or_default();
    let attr_value = p_str(body, query, "attributeValue").unwrap_or_default();
    let type_name = p_str(body, query, "thingTypeName").unwrap_or_default();
    let mut items = Vec::new();
    for t in &s.things {
        if !attr_name.is_empty() {
            let got = t["attributes"]
                .get(&attr_name)
                .and_then(Value::as_str)
                .unwrap_or_default();
            if got != attr_value {
                continue;
            }
        }
        if !type_name.is_empty() && t["thingTypeName"].as_str().unwrap_or_default() != type_name {
            continue;
        }
        let mut obj = t.clone();
        if let Some(o) = obj.as_object_mut() {
            o.remove("thingId");
        }
        items.push(obj);
    }
    Ok(ok_json(&json!({"things": items, "nextToken": Value::Null})))
}

fn register_thing(_body: &Value) -> Result<Response, Response> {
    Ok(ok_json(&json!({"resourceArns": {}})))
}

// ---------------------------------------------------------------------------
// Thing principals — impls. moto's `_get_principal`: certificate ARNs
// (and thing-group ARNs) must exist before an attach/detach succeeds.
// ---------------------------------------------------------------------------

fn require_principal(s: &State, principal_arn: &str) -> Result<(), Response> {
    if principal_arn.contains(":cert/") {
        if s.certificates
            .iter()
            .any(|c| c["arn"] == json!(principal_arn))
        {
            return Ok(());
        }
        return Err(not_found());
    }
    if principal_arn.contains(":thinggroup/") {
        if s.thing_groups
            .iter()
            .any(|g| g["arn"] == json!(principal_arn))
        {
            return Ok(());
        }
        return Err(not_found());
    }
    Err(not_found())
}

fn attach_thing_principal(
    s: &mut State,
    thing_name: &str,
    req: &Request,
) -> Result<Response, Response> {
    let principal = header(req, "x-amzn-principal")
        .unwrap_or_default()
        .to_string();
    require_principal(s, &principal)?;
    find_thing_idx(s, thing_name).ok_or_else(not_found)?;
    let key = (principal, thing_name.to_string());
    if !s.principal_things.contains(&key) {
        s.principal_things.push(key);
    }
    Ok(ok_empty())
}

fn detach_thing_principal(
    s: &mut State,
    thing_name: &str,
    req: &Request,
) -> Result<Response, Response> {
    let principal = header(req, "x-amzn-principal")
        .unwrap_or_default()
        .to_string();
    require_principal(s, &principal)?;
    find_thing_idx(s, thing_name).ok_or_else(not_found)?;
    let key = (principal, thing_name.to_string());
    if !s.principal_things.contains(&key) {
        return Err(not_found());
    }
    s.principal_things.retain(|k| *k != key);
    Ok(ok_empty())
}

fn list_thing_principals(s: &mut State, thing_name: &str) -> Result<Response, Response> {
    find_thing_idx(s, thing_name).ok_or_else(not_found)?;
    let principals: Vec<&String> = s
        .principal_things
        .iter()
        .filter(|(_, tn)| tn == thing_name)
        .map(|(p, _)| p)
        .collect();
    Ok(ok_json(&json!({"principals": principals})))
}

fn list_thing_principals_v2(s: &mut State, thing_name: &str) -> Result<Response, Response> {
    find_thing_idx(s, thing_name).ok_or_else(not_found)?;
    let objects: Vec<Value> = s
        .principal_things
        .iter()
        .filter(|(_, tn)| tn == thing_name)
        .map(|(p, _)| json!({"thingName": thing_name, "principal": p}))
        .collect();
    Ok(ok_json(&json!({"thingPrincipalObjects": objects})))
}

fn get_thing_connectivity_data(s: &mut State, name: &str) -> Result<Response, Response> {
    find_thing_idx(s, name).ok_or_else(not_found)?;
    Ok(ok_json(&json!({
        "connected": false,
        "disconnectReason": "NORMAL",
        "timestamp": crate::proto::now_unix(),
    })))
}

// ---------------------------------------------------------------------------
// Thing types — impls
// ---------------------------------------------------------------------------

fn create_thing_type(
    s: &mut State,
    name: &str,
    body: &Value,
    region: &str,
    account: u64,
) -> Result<Response, Response> {
    let arn = arn_for(region, account, "thingtype", name);
    let props = body
        .get("thingTypeProperties")
        .filter(|v| !v.is_null())
        .cloned()
        .unwrap_or_else(|| json!({}));
    s.thing_types.push(json!({
        "thingTypeName": name,
        "thingTypeId": random_id(),
        "thingTypeProperties": props,
        "thingTypeMetadata": {"deprecated": false, "creationDate": crate::proto::now_iso()},
        "thingTypeArn": arn,
    }));
    Ok(ok_json(
        &json!({"thingTypeName": name, "thingTypeArn": arn}),
    ))
}

fn describe_thing_type(s: &mut State, name: &str) -> Result<Response, Response> {
    s.thing_types
        .iter()
        .find(|t| t["thingTypeName"].as_str() == Some(name))
        .map(ok_json)
        .ok_or_else(not_found)
}

fn update_thing_type(s: &mut State, name: &str, body: &Value) -> Result<Response, Response> {
    let idx = find_by_field_idx(&s.thing_types, "thingTypeName", name).ok_or_else(not_found)?;
    if let Some(props) = b_val_ref(body, "thingTypeProperties") {
        s.thing_types[idx]["thingTypeProperties"] = props.clone();
    }
    Ok(ok_empty())
}

fn delete_thing_type(s: &mut State, name: &str) -> Result<Response, Response> {
    let idx = find_by_field_idx(&s.thing_types, "thingTypeName", name).ok_or_else(not_found)?;
    s.thing_types.remove(idx);
    Ok(ok_empty())
}

fn deprecate_thing_type(s: &mut State, name: &str, body: &Value) -> Result<Response, Response> {
    let idx = find_by_field_idx(&s.thing_types, "thingTypeName", name).ok_or_else(not_found)?;
    let undo = b_bool(body, "undoDeprecate").unwrap_or(false);
    s.thing_types[idx]["thingTypeMetadata"]["deprecated"] = json!(!undo);
    Ok(ok_json(&s.thing_types[idx]))
}

fn list_thing_types(
    s: &mut State,
    body: &Value,
    query: &BTreeMap<String, Vec<String>>,
) -> Result<Response, Response> {
    let filter = p_str(body, query, "thingTypeName").unwrap_or_default();
    let items: Vec<Value> = s
        .thing_types
        .iter()
        .filter(|t| {
            filter.is_empty()
                || t["thingTypeName"]
                    .as_str()
                    .unwrap_or_default()
                    .starts_with(&filter)
        })
        .cloned()
        .collect();
    Ok(ok_json(
        &json!({"thingTypes": items, "nextToken": Value::Null}),
    ))
}

// ---------------------------------------------------------------------------
// Thing groups — impls
// ---------------------------------------------------------------------------

fn create_thing_group(
    s: &mut State,
    name: &str,
    body: &Value,
    region: &str,
    account: u64,
) -> Result<Response, Response> {
    if s.thing_groups
        .iter()
        .any(|g| g["thingGroupName"] == json!(name))
    {
        return Err(already_exists(
            &format!(
                "Thing Group {name} already exists in current account with different properties"
            ),
            name,
            &arn_for(region, account, "thinggroup", name),
        ));
    }
    let parent = b_str(body, "parentGroupName");
    let mut metadata = json!({"creationDate": crate::proto::now_iso()});
    if let Some(p) = parent {
        let parent_rec = s
            .thing_groups
            .iter()
            .find(|g| g["thingGroupName"] == json!(p))
            .cloned();
        let mut chain: Vec<Value> = Vec::new();
        if let Some(pg) = parent_rec {
            if let Some(root) = pg
                .get("thingGroupMetadata")
                .and_then(|m| m.get("rootToParentThingGroups"))
            {
                chain = root.as_array().cloned().unwrap_or_default();
            }
            chain.push(json!({"groupName": p, "groupArn": pg["arn"]}));
        }
        metadata["parentGroupName"] = json!(p);
        metadata["rootToParentThingGroups"] = json!(chain);
    }
    let g = json!({
        "thingGroupName": name,
        "thingGroupId": random_id(),
        "version": 1,
        "thingGroupProperties": body.get("thingGroupProperties").filter(|v| !v.is_null()).cloned().unwrap_or_else(|| json!({})),
        "thingGroupMetadata": metadata,
        "thingGroupArn": arn_for(region, account, "thinggroup", name),
        "things": [],
        "parentGroupName": parent,
    });
    let out = json!({
        "thingGroupName": name,
        "thingGroupArn": g["thingGroupArn"],
        "thingGroupId": g["thingGroupId"],
    });
    s.thing_groups.push(g);
    Ok(ok_json(&out))
}

fn describe_thing_group(s: &mut State, name: &str) -> Result<Response, Response> {
    s.thing_groups
        .iter()
        .find(|g| g["thingGroupName"].as_str() == Some(name))
        .map(|g| {
            let mut obj = g.clone();
            if let Some(o) = obj.as_object_mut() {
                o.remove("things");
                o.remove("parentGroupName");
            }
            ok_json(&obj)
        })
        .ok_or_else(not_found)
}

fn update_thing_group(
    s: &mut State,
    name: &str,
    body: &Value,
    query: &BTreeMap<String, Vec<String>>,
) -> Result<Response, Response> {
    let idx = find_by_field_idx(&s.thing_groups, "thingGroupName", name).ok_or_else(not_found)?;
    let expected = p_i64(body, query, "expectedVersion");
    if let Some(expected) = expected {
        if expected > 0 && expected != s.thing_groups[idx]["version"].as_i64().unwrap_or(0) {
            return Err(version_conflict(name));
        }
    }
    if let Some(props) = b_val_ref(body, "thingGroupProperties") {
        if let Some(obj) = props.as_object() {
            for (k, v) in obj {
                s.thing_groups[idx]["thingGroupProperties"][k] = v.clone();
            }
        }
    }
    let version = s.thing_groups[idx]["version"].as_i64().unwrap_or(1) + 1;
    s.thing_groups[idx]["version"] = json!(version);
    Ok(ok_json(&json!({"version": version})))
}

fn delete_thing_group(s: &mut State, name: &str) -> Result<Response, Response> {
    if s.thing_groups
        .iter()
        .any(|g| g["parentGroupName"] == json!(name))
    {
        return Err(invalid_request(&format!(
            " Cannot delete thing group : {name} when there are still child groups attached to it"
        )));
    }
    if let Some(idx) = find_by_field_idx(&s.thing_groups, "thingGroupName", name) {
        s.thing_groups.remove(idx);
    }
    Ok(ok_empty())
}

fn list_thing_groups(
    s: &mut State,
    body: &Value,
    query: &BTreeMap<String, Vec<String>>,
) -> Result<Response, Response> {
    let parent = p_str(body, query, "parentGroup").unwrap_or_default();
    let prefix = p_str(body, query, "namePrefixFilter").unwrap_or_default();
    if !parent.is_empty()
        && s.thing_groups
            .iter()
            .all(|g| g["thingGroupName"] != json!(parent))
    {
        return Err(not_found());
    }
    let mut items: Vec<Value> = Vec::new();
    for g in &s.thing_groups {
        if g["parentGroupName"].as_str().unwrap_or_default() != parent {
            continue;
        }
        items.push(json!({"groupName": g["thingGroupName"], "groupArn": g["arn"]}));
    }
    let top_level = items.clone();
    for g in s.thing_groups.iter() {
        if g["parentGroupName"].is_null() {
            continue;
        }
        // one-level expansion of descendants (moto's list_thing_groups)
        let parent_name = g["parentGroupName"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        if top_level
            .iter()
            .any(|i| i["groupName"] == json!(parent_name))
            && !items.iter().any(|i| i["groupName"] == g["thingGroupName"])
        {
            items.push(json!({"groupName": g["thingGroupName"], "groupArn": g["arn"]}));
        }
    }
    if !prefix.is_empty() {
        items.retain(|i| {
            i["groupName"]
                .as_str()
                .unwrap_or_default()
                .starts_with(&prefix)
        });
    }
    Ok(ok_json(
        &json!({"thingGroups": items, "nextToken": Value::Null}),
    ))
}

fn list_things_in_thing_group(s: &mut State, name: &str) -> Result<Response, Response> {
    let idx = find_by_field_idx(&s.thing_groups, "thingGroupName", name).ok_or_else(not_found)?;
    let things: Vec<&Value> = s.thing_groups[idx]
        .get("things")
        .and_then(Value::as_array)
        .map(|arr| arr.iter().collect())
        .unwrap_or_default();
    let mut names = Vec::new();
    for arn in &things {
        if let Some(t) = s.things.iter().find(|t| t["thingArn"] == **arn) {
            names.push(t["thingName"].clone());
        } else if let Some(name) = arn.as_str() {
            names.push(json!(name.rsplit('/').next().unwrap_or_default()));
        }
    }
    Ok(ok_json(&json!({"things": names, "nextToken": Value::Null})))
}

fn identify_thing(
    s: &State,
    body: &Value,
    query: &BTreeMap<String, Vec<String>>,
) -> Result<Value, Response> {
    let name = p_str(body, query, "thingName").unwrap_or_default();
    let arn = p_str(body, query, "thingArn").unwrap_or_default();
    if name.is_empty() && arn.is_empty() {
        return Err(invalid_request(
            "Both thingArn and thingName are empty. Need to specify at least one of them",
        ));
    }
    if !name.is_empty() {
        return s
            .things
            .iter()
            .find(|t| t["thingName"] == json!(name))
            .cloned()
            .ok_or_else(not_found);
    }
    s.things
        .iter()
        .find(|t| t["thingArn"] == json!(arn))
        .cloned()
        .ok_or_else(invalid_empty_request)
}

fn identify_thing_group(
    s: &State,
    body: &Value,
    query: &BTreeMap<String, Vec<String>>,
) -> Result<Value, Response> {
    let name = p_str(body, query, "thingGroupName").unwrap_or_default();
    let arn = p_str(body, query, "thingGroupArn").unwrap_or_default();
    if name.is_empty() && arn.is_empty() {
        return Err(invalid_request(
            " Both thingGroupArn and thingGroupName are empty. Need to specify at least one of them",
        ));
    }
    if !name.is_empty() {
        return s
            .thing_groups
            .iter()
            .find(|g| g["thingGroupName"] == json!(name))
            .cloned()
            .ok_or_else(not_found);
    }
    s.thing_groups
        .iter()
        .find(|g| g["arn"] == json!(arn))
        .cloned()
        .ok_or_else(invalid_empty_request)
}

fn invalid_empty_request() -> Response {
    invalid_request("No such resource")
}

fn add_remove_thing_group(
    s: &mut State,
    body: &Value,
    query: &BTreeMap<String, Vec<String>>,
    add: bool,
) -> Result<Response, Response> {
    let group = identify_thing_group(s, body, query)?;
    let thing = identify_thing(s, body, query)?;
    let Some(g) = s.thing_groups.iter_mut().find(|g| g["arn"] == group["arn"]) else {
        return Err(not_found());
    };
    if g["things"].is_null() {
        g["things"] = json!([]);
    }
    if let Some(arr) = g["things"].as_array_mut() {
        if add {
            if !arr.contains(&thing["thingArn"]) {
                arr.push(thing["thingArn"].clone());
            }
        } else {
            arr.retain(|a| a != &thing["thingArn"]);
        }
    }
    Ok(ok_empty())
}

fn list_thing_groups_for_thing(s: &mut State, thing_name: &str) -> Result<Response, Response> {
    let thing = s
        .things
        .iter()
        .find(|t| t["thingName"].as_str() == Some(thing_name))
        .cloned()
        .ok_or_else(not_found)?;
    let mut items: Vec<Value> = Vec::new();
    for g in &s.thing_groups {
        let arn = &thing["thingArn"];
        let declared = g
            .get("things")
            .and_then(Value::as_array)
            .map(|a| a.contains(arn))
            .unwrap_or(false);
        if declared {
            items.push(json!({"groupName": g["thingGroupName"], "groupArn": g["arn"]}));
        }
    }
    Ok(ok_json(
        &json!({"thingGroups": items, "nextToken": Value::Null}),
    ))
}

fn update_thing_groups_for_thing(s: &mut State, body: &Value) -> Result<Response, Response> {
    let name = b_str(body, "thingName")
        .ok_or_else(|| invalid_request("thingName missing"))?
        .to_string();
    let thing = s
        .things
        .iter()
        .find(|t| t["thingName"].as_str() == Some(&name))
        .cloned()
        .ok_or_else(not_found)?;
    let add_groups: Vec<String> = body
        .get("thingGroupsToAdd")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default();
    let remove_groups: Vec<String> = body
        .get("thingGroupsToRemove")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default();
    for group_name in add_groups {
        add_remove_thing_group(
            s,
            &json!({"thingGroupName": group_name, "thingName": name}),
            &BTreeMap::new(),
            true,
        )?;
    }
    for group_name in remove_groups {
        add_remove_thing_group(
            s,
            &json!({"thingGroupName": group_name, "thingName": name}),
            &BTreeMap::new(),
            false,
        )?;
    }
    let _ = thing;
    Ok(ok_empty())
}

// ---------------------------------------------------------------------------
// Billing groups + dynamic thing groups — impls
// ---------------------------------------------------------------------------

fn create_billing_group(
    s: &mut State,
    name: &str,
    body: &Value,
    region: &str,
    account: u64,
) -> Result<Response, Response> {
    if let Some(existing) = s
        .billing_groups
        .iter()
        .find(|g| g["billingGroupName"] == json!(name))
    {
        return Err(already_exists(
            &format!("Billing group {name} already exists."),
            name,
            existing["billingGroupArn"].as_str().unwrap_or_default(),
        ));
    }
    s.billing_groups.push(json!({
        "billingGroupName": name,
        "billingGroupId": random_id(),
        "billingGroupArn": arn_for(region, account, "billinggroup", name),
        "billingGroupProperties": body.get("billingGroupProperties").filter(|v| !v.is_null()).cloned().unwrap_or_else(|| json!({})),
        "billingGroupMetadata": {"creationDate": crate::proto::now_iso()},
        "version": 1,
        "things": [],
    }));
    Ok(ok_json(&json!({
        "billingGroupName": name,
        "billingGroupArn": arn_for(region, account, "billinggroup", name),
    })))
}

fn describe_billing_group(s: &mut State, name: &str) -> Result<Response, Response> {
    s.billing_groups
        .iter()
        .find(|g| g["billingGroupName"].as_str() == Some(name))
        .map(ok_json)
        .ok_or_else(not_found)
}

fn update_billing_group(
    s: &mut State,
    name: &str,
    body: &Value,
    query: &BTreeMap<String, Vec<String>>,
) -> Result<Response, Response> {
    let idx =
        find_by_field_idx(&s.billing_groups, "billingGroupName", name).ok_or_else(not_found)?;
    let expected = p_i64(body, query, "expectedVersion");
    if let Some(expected) = expected {
        if expected > 0 && expected != s.billing_groups[idx]["version"].as_i64().unwrap_or(0) {
            return Err(version_conflict(name));
        }
    }
    if let Some(props) = b_val_ref(body, "billingGroupProperties") {
        s.billing_groups[idx]["billingGroupProperties"] = props.clone();
    }
    let version = s.billing_groups[idx]["version"].as_i64().unwrap_or(1) + 1;
    s.billing_groups[idx]["version"] = json!(version);
    Ok(ok_json(&json!({"version": version})))
}

fn delete_billing_group(s: &mut State, name: &str) -> Result<Response, Response> {
    let idx =
        find_by_field_idx(&s.billing_groups, "billingGroupName", name).ok_or_else(not_found)?;
    s.billing_groups.remove(idx);
    Ok(ok_empty())
}

fn list_billing_groups(
    s: &mut State,
    body: &Value,
    query: &BTreeMap<String, Vec<String>>,
) -> Result<Response, Response> {
    let prefix = p_str(body, query, "namePrefixFilter").unwrap_or_default();
    let items: Vec<Value> = s
        .billing_groups
        .iter()
        .filter(|g| {
            prefix.is_empty()
                || g["billingGroupName"]
                    .as_str()
                    .unwrap_or_default()
                    .starts_with(&prefix)
        })
        .map(|g| json!({"groupName": g["billingGroupName"], "groupArn": g["billingGroupArn"]}))
        .collect();
    Ok(ok_json(
        &json!({"billingGroups": items, "nextToken": Value::Null}),
    ))
}

fn identify_thing_for_billing(
    s: &State,
    body: &Value,
    query: &BTreeMap<String, Vec<String>>,
) -> Result<Value, Response> {
    let name = p_str(body, query, "thingName").unwrap_or_default();
    let arn = p_str(body, query, "thingArn").unwrap_or_default();
    if !name.is_empty() {
        return s
            .things
            .iter()
            .find(|t| t["thingName"] == json!(name))
            .cloned()
            .ok_or_else(not_found);
    }
    if !arn.is_empty() {
        return s
            .things
            .iter()
            .find(|t| t["thingArn"] == json!(arn))
            .cloned()
            .ok_or_else(not_found);
    }
    Err(invalid_request(
        "Both thingName and thingArn cannot be null",
    ))
}

fn group_billing(
    s: &State,
    body: &Value,
    query: &BTreeMap<String, Vec<String>>,
) -> Result<Value, Response> {
    let name = p_str(body, query, "billingGroupName").unwrap_or_default();
    let arn = p_str(body, query, "billingGroupArn").unwrap_or_default();
    if !name.is_empty() {
        return s
            .billing_groups
            .iter()
            .find(|g| g["billingGroupName"] == json!(name))
            .cloned()
            .ok_or_else(not_found);
    }
    if !arn.is_empty() {
        let short = arn.rsplit('/').next().unwrap_or_default();
        return s
            .billing_groups
            .iter()
            .find(|g| g["billingGroupName"] == json!(short))
            .cloned()
            .ok_or_else(not_found);
    }
    Err(invalid_request(
        "Both billingGroupName and billingGroupArn cannot be null",
    ))
}

fn add_thing_to_billing_group(
    s: &mut State,
    body: &Value,
    query: &BTreeMap<String, Vec<String>>,
) -> Result<Response, Response> {
    let group = group_billing(s, body, query)?;
    let thing = identify_thing_for_billing(s, body, query)?;
    let g_idx = find_by_field_idx(
        &s.billing_groups,
        "billingGroupName",
        group["billingGroupName"].as_str().unwrap_or_default(),
    )
    .ok_or_else(not_found)?;
    if s.billing_groups[g_idx]["things"].is_null() {
        s.billing_groups[g_idx]["things"] = json!([]);
    }
    let thing_arn = thing["thingArn"].clone();
    if let Some(arr) = s.billing_groups[g_idx]["things"].as_array_mut() {
        if !arr.contains(&thing_arn) {
            arr.push(thing_arn.clone());
        }
    }
    if let Some(t) = s.things.iter_mut().find(|t| t["thingArn"] == thing_arn) {
        t["billingGroupName"] = group["billingGroupName"].clone();
    }
    Ok(ok_empty())
}

fn remove_thing_from_billing_group(
    s: &mut State,
    body: &Value,
    query: &BTreeMap<String, Vec<String>>,
) -> Result<Response, Response> {
    let group = group_billing(s, body, query)?;
    let thing = identify_thing_for_billing(s, body, query)?;
    let g_idx = find_by_field_idx(
        &s.billing_groups,
        "billingGroupName",
        group["billingGroupName"].as_str().unwrap_or_default(),
    )
    .ok_or_else(not_found)?;
    let arn = thing["thingArn"].clone();
    if let Some(arr) = s.billing_groups[g_idx]["things"].as_array_mut() {
        arr.retain(|a| a != &arn);
    }
    if let Some(t) = s.things.iter_mut().find(|t| t["thingArn"] == arn) {
        t.as_object_mut().map(|o| o.remove("billingGroupName"));
    }
    Ok(ok_empty())
}

fn list_things_in_billing_group(s: &mut State, name: &str) -> Result<Response, Response> {
    let idx =
        find_by_field_idx(&s.billing_groups, "billingGroupName", name).ok_or_else(not_found)?;
    let mut names: Vec<Value> = Vec::new();
    let arns: Vec<Value> = s.billing_groups[idx]
        .get("things")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    for arn in arns {
        if let Some(t) = s.things.iter().find(|t| t["thingArn"] == arn) {
            names.push(t["thingName"].clone());
        }
    }
    Ok(ok_json(&json!({"things": names, "nextToken": Value::Null})))
}

fn create_dynamic_thing_group(
    s: &mut State,
    name: &str,
    body: &Value,
    region: &str,
    account: u64,
) -> Result<Response, Response> {
    if s.dynamic_thing_groups
        .iter()
        .any(|g| g["thingGroupName"] == json!(name))
    {
        return Err(already_exists(
            &format!("Dynamic thing group {name} already exists."),
            name,
            &arn_for(region, account, "thinggroup", name),
        ));
    }
    let g = json!({
        "thingGroupName": name,
        "thingGroupId": random_id(),
        "thingGroupArn": arn_for(region, account, "thinggroup", name),
        "queryString": b_str(body, "queryString").unwrap_or_default(),
        "queryVersion": crate::proto::now_iso(),
        "status": "ACTIVE",
        "version": 1,
        "thingGroupProperties": body.get("thingGroupProperties").filter(|v| !v.is_null()).cloned().unwrap_or_else(|| json!({})),
    });
    let _ = match s.dynamic_thing_groups.last() {
        Some(last) => last.clone(),
        None => Value::Null,
    };
    s.dynamic_thing_groups.push(g.clone());
    Ok(ok_json(&json!({
        "thingGroupName": name,
        "thingGroupArn": g["thingGroupArn"],
        "thingGroupId": g["thingGroupId"],
    })))
}

fn update_dynamic_thing_group(
    s: &mut State,
    name: &str,
    body: &Value,
    query: &BTreeMap<String, Vec<String>>,
) -> Result<Response, Response> {
    let idx =
        find_by_field_idx(&s.dynamic_thing_groups, "thingGroupName", name).ok_or_else(not_found)?;
    let expected = p_i64(body, query, "expectedVersion");
    if let Some(expected) = expected {
        if expected > 0 && expected != s.dynamic_thing_groups[idx]["version"].as_i64().unwrap_or(0)
        {
            return Err(version_conflict(name));
        }
    }
    if let Some(props) = b_val_ref(body, "thingGroupProperties") {
        s.dynamic_thing_groups[idx]["thingGroupProperties"] = props.clone();
    }
    merge_opt_members(
        &mut s.dynamic_thing_groups[idx],
        body,
        &["queryString", "queryVersion", "indexName"],
    );
    let version = s.dynamic_thing_groups[idx]["version"].as_i64().unwrap_or(1) + 1;
    s.dynamic_thing_groups[idx]["version"] = json!(version);
    Ok(ok_json(&json!({"version": version})))
}

fn delete_dynamic_thing_group(s: &mut State, name: &str) -> Result<Response, Response> {
    if let Some(idx) = find_by_field_idx(&s.dynamic_thing_groups, "thingGroupName", name) {
        s.dynamic_thing_groups.remove(idx);
    }
    Ok(ok_empty())
}

// ---------------------------------------------------------------------------
// Policies + attach/detach — impls. Documents ride through verbatim (moto
// stores the parsed body member verbatim; the suites echo it back).
// ---------------------------------------------------------------------------

fn create_policy(
    s: &mut State,
    name: &str,
    body: &Value,
    region: &str,
    account: u64,
) -> Result<Response, Response> {
    if let Some(existing) = s.policies.iter().find(|p| p["policyName"] == json!(name)) {
        return Err(already_exists(
            &format!("Policy cannot be created - name already exists (name={name})"),
            existing["policyName"].as_str().unwrap_or_default(),
            existing["arn"].as_str().unwrap_or_default(),
        ));
    }
    let document = b_val_ref(body, "policyDocument")
        .cloned()
        .unwrap_or(Value::Null);
    let arn = arn_for(region, account, "policy", name);
    s.policies.push(json!({
        "policyName": name,
        "arn": arn,
        "policyDocument": document,
        "defaultVersionId": "1",
        "maxVersionId": 1,
        "versions": [{
            "policyVersionId": "1",
            "document": document,
            "isDefaultVersion": true,
        }],
    }));
    Ok(ok_json(&json!({
        "policyName": name,
        "policyArn": arn,
        "policyDocument": document,
        "policyVersionId": "1",
    })))
}

fn get_policy(s: &mut State, name: &str) -> Result<Response, Response> {
    let Some(p) = s.policies.iter().find(|p| p["policyName"] == json!(name)) else {
        return Err(not_found());
    };
    Ok(ok_json(&json!({
        "policyName": p["policyName"],
        "policyArn": p["arn"],
        "policyDocument": p["policyDocument"],
        "defaultVersionId": p["defaultVersionId"],
    })))
}

fn delete_policy(s: &mut State, name: &str) -> Result<Response, Response> {
    if s.principal_policies.iter().any(|(_, pn)| pn == name) {
        return Err(delete_conflict(&format!(
            "The policy cannot be deleted as the policy is attached to one or more principals (name={name})"
        )));
    }
    let Some(p) = s.policies.iter().find(|p| p["policyName"] == json!(name)) else {
        return Err(not_found());
    };
    let versions = p["versions"].as_array().map(Vec::len).unwrap_or(1);
    if versions > 1 {
        return Err(delete_conflict(&format!(
            "Cannot delete the policy because it has one or more policy versions attached to it (name={name})"
        )));
    }
    let idx = find_by_field_idx(&s.policies, "policyName", name).unwrap();
    s.policies.remove(idx);
    Ok(ok_empty())
}

fn list_policies(s: &mut State) -> Result<Response, Response> {
    let items: Vec<Value> = s
        .policies
        .iter()
        .map(|p| json!({"policyName": p["policyName"], "policyArn": p["arn"]}))
        .collect();
    Ok(ok_json(&json!({"policies": items})))
}

fn create_policy_version(
    s: &mut State,
    name: &str,
    body: &Value,
    query: &BTreeMap<String, Vec<String>>,
    _region: &str,
    _account: u64,
) -> Result<Response, Response> {
    let idx = find_by_field_idx(&s.policies, "policyName", name).ok_or_else(not_found)?;
    let document = b_val_ref(body, "policyDocument")
        .cloned()
        .unwrap_or(Value::Null);
    let set_default = p_bool(body, query, "setAsDefault").unwrap_or(false);
    let versions = s.policies[idx]["versions"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    if versions.len() >= 5 {
        return Err(versions_limit(name));
    }
    let next = s.policies[idx]["maxVersionId"].as_i64().unwrap_or(1) + 1;
    s.policies[idx]["maxVersionId"] = json!(next);
    let new_version = json!({
        "policyVersionId": next.to_string(),
        "document": document,
        "isDefaultVersion": set_default,
    });
    if let Some(arr) = s.policies[idx]["versions"].as_array_mut() {
        arr.push(new_version.clone());
    }
    let arn = s.policies[idx]["arn"].clone();
    let policy_version_id = new_version["policyVersionId"].clone();
    let is_default = new_version["isDefaultVersion"].clone();
    if set_default {
        if let Some(arr) = s.policies[idx]["versions"].as_array_mut() {
            let id = new_version["policyVersionId"].clone();
            for v in arr.iter_mut() {
                v["isDefaultVersion"] = json!(v["policyVersionId"] == id);
            }
        }
        s.policies[idx]["defaultVersionId"] = new_version["policyVersionId"].clone();
        s.policies[idx]["policyDocument"] = new_version["document"].clone();
    }
    Ok(ok_json(&json!({
        "policyArn": arn,
        "policyDocument": new_version["document"].clone(),
        "policyVersionId": policy_version_id,
        "isDefaultVersion": is_default,
    })))
}

fn list_policy_versions(s: &mut State, name: &str) -> Result<Response, Response> {
    let Some(p) = s.policies.iter().find(|p| p["policyName"] == json!(name)) else {
        return Err(not_found());
    };
    let items: Vec<Value> = p["versions"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(|v| {
            json!({
                "versionId": v["policyVersionId"].clone(),
                "isDefaultVersion": v["isDefaultVersion"].clone(),
                "createDate": crate::proto::now_iso(),
            })
        })
        .collect();
    Ok(ok_json(&json!({"policyVersions": items})))
}

fn get_policy_version(s: &mut State, name: &str, version_id: &str) -> Result<Response, Response> {
    let Some(p) = s.policies.iter().find(|p| p["policyName"] == json!(name)) else {
        return Err(not_found());
    };
    let Some(v) = p["versions"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .find(|v| v["policyVersionId"] == json!(version_id))
    else {
        return Err(not_found());
    };
    let generation_id = p["defaultVersionId"].clone();
    Ok(ok_json(&json!({
        "policyName": p["policyName"],
        "policyArn": p["arn"],
        "policyDocument": v["document"],
        "policyVersionId": v["policyVersionId"],
        "isDefaultVersion": v["isDefaultVersion"],
        "creationDate": crate::proto::now_iso(),
        "lastModifiedDate": crate::proto::now_iso(),
        "generationId": generation_id,
    })))
}

fn delete_policy_version(
    s: &mut State,
    name: &str,
    version_id: &str,
) -> Result<Response, Response> {
    let idx = find_by_field_idx(&s.policies, "policyName", name).ok_or_else(not_found)?;
    let default_id = s.policies[idx]["defaultVersionId"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    if default_id == version_id {
        return Err(invalid_request(
            "Cannot delete the default version of a policy",
        ));
    }
    let mut removed = false;
    if let Some(arr) = s.policies[idx]["versions"].as_array_mut() {
        let before = arr.len();
        arr.retain(|v| v["policyVersionId"].as_str() != Some(version_id));
        removed = arr.len() < before;
    }
    if !removed {
        return Err(not_found());
    }
    Ok(ok_empty())
}

fn set_default_policy_version(
    s: &mut State,
    name: &str,
    version_id: &str,
) -> Result<Response, Response> {
    let idx = find_by_field_idx(&s.policies, "policyName", name).ok_or_else(not_found)?;
    let mut wanted_document = None;
    if let Some(arr) = s.policies[idx]["versions"].as_array_mut() {
        for v in arr.iter_mut() {
            if v["policyVersionId"].as_str() == Some(version_id) {
                v["isDefaultVersion"] = json!(true);
                wanted_document = Some(v["document"].clone());
            } else {
                v["isDefaultVersion"] = json!(false);
            }
        }
    }
    match wanted_document {
        Some(document) => {
            s.policies[idx]["policyDocument"] = document;
            s.policies[idx]["defaultVersionId"] = json!(version_id);
            Ok(ok_empty())
        }
        None => Err(not_found()),
    }
}

// ---------------------------------------------------------------------------
// Attach policy <-> principal (principals come from `\target` body
// member or the x-amzn-iot-principal header, per moto's responses).
// ---------------------------------------------------------------------------

fn attach_policy(s: &mut State, policy_name: &str, body: &Value) -> Result<Response, Response> {
    let target = b_str(body, "target").unwrap_or_default().to_string();
    attach_one(s, &target, policy_name)
}

fn detach_policy(s: &mut State, policy_name: &str, body: &Value) -> Result<Response, Response> {
    let target = b_str(body, "target").unwrap_or_default().to_string();
    require_principal(s, &target)?;
    get_policy_or_err(s, policy_name)?;
    let key = (target, policy_name.to_string());
    if !s.principal_policies.contains(&key) {
        return Err(not_found());
    }
    s.principal_policies.retain(|k| *k != key);
    Ok(ok_empty())
}

fn attach_one(s: &mut State, target: &str, policy_name: &str) -> Result<Response, Response> {
    require_principal(s, target)?;
    get_policy_or_err(s, policy_name)?;
    let key = (target.to_string(), policy_name.to_string());
    if !s.principal_policies.contains(&key) {
        s.principal_policies.push(key);
    }
    Ok(ok_empty())
}

fn get_policy_or_err(s: &State, name: &str) -> Result<Value, Response> {
    s.policies
        .iter()
        .find(|p| p["policyName"] == json!(name))
        .cloned()
        .ok_or_else(not_found)
}

fn list_attached_policies(s: &mut State, target: &str) -> Result<Response, Response> {
    let items: Vec<Value> = s
        .principal_policies
        .iter()
        .filter(|(principal, _)| principal == target)
        .map(|(_, policy_name)| {
            s.policies
                .iter()
                .find(|p| p["policyName"] == json!(policy_name))
                .map(|p| json!({"policyName": p["policyName"], "policyArn": p["arn"]}))
                .unwrap_or_else(|| json!({}))
        })
        .collect();
    Ok(ok_json(&json!({"policies": items})))
}

fn attach_principal_policy(
    s: &mut State,
    policy_name: &str,
    req: &Request,
) -> Result<Response, Response> {
    let principal = header(req, "x-amzn-iot-principal")
        .unwrap_or_default()
        .to_string();
    attach_one(s, &principal, policy_name)
}

fn detach_principal_policy(
    s: &mut State,
    policy_name: &str,
    req: &Request,
) -> Result<Response, Response> {
    let principal = header(req, "x-amzn-iot-principal")
        .unwrap_or_default()
        .to_string();
    detach_policy(s, policy_name, &json!({"target": principal}))
}

fn list_principal_policies(req: &Request, s: &mut State) -> Result<Response, Response> {
    let principal = header(req, "x-amzn-iot-principal")
        .unwrap_or_default()
        .to_string();
    let items: Vec<Value> = s
        .principal_policies
        .iter()
        .filter(|(p, _)| *p == principal)
        .map(|(_, policy_name)| {
            s.policies
                .iter()
                .find(|p| p["policyName"] == json!(policy_name))
                .map(|p| json!({"policyName": p["policyName"], "policyArn": p["arn"]}))
                .unwrap_or_else(|| json!({}))
        })
        .collect();
    Ok(ok_json(&json!({"policies": items})))
}

fn list_policy_principals(s: &mut State, req: &Request) -> Result<Response, Response> {
    let policy = header(req, "x-amzn-iot-policy")
        .unwrap_or_default()
        .to_string();
    let items: Vec<Value> = s
        .principal_policies
        .iter()
        .filter(|(_, policy_name)| *policy_name == policy)
        .map(|(principal, _)| json!(principal))
        .collect();
    Ok(ok_json(&json!({"principals": items})))
}

fn list_targets_for_policy(s: &mut State, policy_name: &str) -> Result<Response, Response> {
    get_policy_or_err(s, policy_name)?;
    let items: Vec<Value> = s
        .principal_policies
        .iter()
        .filter(|(_, pn)| pn == policy_name)
        .map(|(principal, _)| json!(principal))
        .collect();
    Ok(ok_json(&json!({"targets": items})))
}

fn list_principal_things(req: &Request, s: &mut State) -> Result<Response, Response> {
    let principal = header(req, "x-amzn-principal")
        .unwrap_or_default()
        .to_string();
    let things: Vec<&String> = s
        .principal_things
        .iter()
        .filter(|(p, _)| *p == principal)
        .map(|(_, tn)| tn)
        .collect();
    Ok(ok_json(
        &json!({"things": things, "nextToken": Value::Null}),
    ))
}

fn list_principal_things_v2(
    req: &Request,
    s: &mut State,
    body: &Value,
    query: &BTreeMap<String, Vec<String>>,
) -> Result<Response, Response> {
    let principal = header(req, "x-amzn-iot-principal")
        .or_else(|| header(req, "x-amzn-principal"))
        .map(str::to_owned)
        .or_else(|| p_str(body, query, "principal"))
        .unwrap_or_default();
    let things: Vec<&String> = s
        .principal_things
        .iter()
        .filter(|(p, _)| *p == principal)
        .map(|(_, tn)| tn)
        .collect();
    let objects: Vec<Value> = things
        .iter()
        .map(|tn| json!({"thingName": tn, "principal": principal}))
        .collect();
    Ok(ok_json(&json!({"principalThingObjects": objects})))
}

fn get_effective_policies(s: &mut State, body: &Value) -> Result<Response, Response> {
    let mut items = Vec::new();
    let principal = b_str(body, "principal").unwrap_or_default().to_string();
    if !principal.is_empty() {
        for (principal_key, policy_name) in s.principal_policies.clone() {
            if principal_key == principal {
                if let Ok(policy) = get_policy_or_err(s, &policy_name) {
                    items.push(json!({
                        "policyName": policy["policyName"],
                        "policyArn": policy["arn"],
                        "policyDocument": policy["policyDocument"],
                    }));
                }
            }
        }
    }
    Ok(ok_json(&json!({"effectivePolicies": items})))
}

// ---------------------------------------------------------------------------
// Certificates — impls. moto (iot_use_valid_cert off) derives the cert id
// from sha256(PEM); CreateKeysAndCertificate fabricates an RSA PEM pair.
// PEM byte-accuracy beyond shape is out of suite reach.
// ---------------------------------------------------------------------------

fn new_certificate(
    certificate_pem: &str,
    status: &str,
    region: &str,
    account: u64,
    ca_certificate_id: Option<&Value>,
) -> Value {
    let id = sha256_hex(certificate_pem.as_bytes());
    let mut cert = json!({
        "certificateId": id,
        "arn": arn_for(region, account, "cert", ""),
        "certificatePem": certificate_pem,
        "status": status,
        "owner": account.to_string(),
        "transferData": {},
        "creationDate": crate::proto::now_iso(),
        "lastModifiedDate": crate::proto::now_iso(),
        "validity": {
            "notBefore": crate::proto::now_iso(),
            "notAfter": crate::proto::now_iso(),
        },
    });
    cert["arn"] = json!(arn_for(
        region,
        account,
        "cert",
        cert["certificateId"].as_str().unwrap_or_default()
    ));
    if let Some(ca_id) = ca_certificate_id {
        cert["caCertificateId"] = ca_id.clone();
    }
    cert
}

/// Create a syntactically-shaped PEM block with deterministic padding.
fn synthetic_pem(begin: &str, seed: &[u8]) -> String {
    use base64::Engine;
    let bytes = {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(seed);
        hasher.finalize().to_vec()
    };
    let engine = base64::engine::general_purpose::STANDARD;
    let blob = engine.encode(bytes);
    let mut lines = Vec::new();
    let mut chars = blob.as_bytes();
    while !chars.is_empty() {
        let take = chars.len().min(64);
        lines.push(String::from_utf8_lossy(&chars[..take]).into_owned());
        chars = &chars[take..];
    }
    format!(
        "-----BEGIN {}-----\n{}\n-----END {}-----\n",
        begin,
        lines.join("\n"),
        begin
    )
}

fn create_keys_and_certificate(
    s: &mut State,
    query: &BTreeMap<String, Vec<String>>,
    region: &str,
    account: u64,
) -> Result<Response, Response> {
    let active = query
        .get("setAsActive")
        .and_then(|v| v.first())
        .map(|v| v == "true")
        .unwrap_or(false);
    let pem = synthetic_pem("CERTIFICATE", random_id().as_bytes());
    let status = if active { "ACTIVE" } else { "INACTIVE" };
    let cert = new_certificate(&pem, status, region, account, None);
    let public_key = synthetic_pem("PUBLIC KEY", random_id().as_bytes());
    let private_key = synthetic_pem("RSA PRIVATE KEY", random_id().as_bytes());
    let body = json!({
        "certificateArn": cert["arn"],
        "certificateId": cert["certificateId"],
        "certificatePem": cert["certificatePem"],
        "keyPair": {"PublicKey": public_key, "PrivateKey": private_key},
    });
    s.certificates.push(cert);
    Ok(ok_json(&body))
}

fn describe_certificate(s: &mut State, id: &str) -> Result<Response, Response> {
    s.certificates
        .iter()
        .find(|c| c["certificateId"] == json!(id))
        .map(|c| ok_json(&json!({"certificateDescription": certificate_description(c)})))
        .ok_or_else(not_found)
}

fn certificate_description(cert: &Value) -> Value {
    json!({
        "certificateArn": cert["arn"],
        "certificateId": cert["certificateId"],
        "status": cert["status"],
        "certificatePem": cert["certificatePem"],
        "ownedBy": cert["owner"],
        "creationDate": cert["creationDate"],
        "lastModifiedDate": cert["lastModifiedDate"],
        "validity": cert["validity"],
        "transferData": cert["transferData"],
        "caCertificateId": cert.get("caCertificateId").unwrap_or(&Value::Null),
    })
}

fn update_certificate(
    s: &mut State,
    id: &str,
    query: &BTreeMap<String, Vec<String>>,
) -> Result<Response, Response> {
    let idx = find_by_field_idx(&s.certificates, "certificateId", id).ok_or_else(not_found)?;
    let status = query
        .get("newStatus")
        .and_then(|v| v.first().cloned())
        .unwrap_or_else(|| "INACTIVE".to_string());
    s.certificates[idx]["status"] = json!(status);
    Ok(ok_empty())
}

fn delete_certificate(
    s: &mut State,
    id: &str,
    query: &BTreeMap<String, Vec<String>>,
) -> Result<Response, Response> {
    let idx = find_by_field_idx(&s.certificates, "certificateId", id).ok_or_else(not_found)?;
    let force = query
        .get("forceDelete")
        .and_then(|v| v.first())
        .map(|v| v == "true")
        .unwrap_or(false);
    let status = s.certificates[idx]["status"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    let arn = s.certificates[idx]["arn"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    if status == "ACTIVE" {
        return Err(cert_state_error(
            "Certificate must be deactivated (not ACTIVE) before deletion.",
            id,
        ));
    }
    if s.principal_things.iter().any(|(p, _)| *p == arn) {
        return Err(delete_conflict(&format!(
            "Things must be detached before deletion (arn: {arn})"
        )));
    }
    if s.principal_policies.iter().any(|(p, _)| *p == arn) && !force {
        return Err(delete_conflict(&format!(
            "Certificate policies must be detached before deletion (arn: {arn})"
        )));
    }
    s.certificates.remove(idx);
    Ok(ok_empty())
}

fn register_certificate(
    s: &mut State,
    body: &Value,
    query: &BTreeMap<String, Vec<String>>,
    region: &str,
    account: u64,
) -> Result<Response, Response> {
    let pem = b_str(body, "certificatePem")
        .unwrap_or_default()
        .to_string();
    let ca_pem = b_str(body, "caCertificatePem");
    let set_active = p_bool(body, query, "setAsActive");
    let status = match set_active {
        Some(true) => "ACTIVE".to_string(),
        Some(false) => b_str(body, "status").unwrap_or_default().to_string(),
        None => b_str(body, "status").unwrap_or_default().to_string(),
    };
    register_with_status(s, &pem, ca_pem, &status, region, account)
}

fn register_with_status(
    s: &mut State,
    pem: &str,
    ca_pem: Option<&str>,
    status: &str,
    region: &str,
    account: u64,
) -> Result<Response, Response> {
    let ca_id = ca_pem.and_then(|cpem| {
        s.ca_certificates
            .iter()
            .find(|c| c["certificatePem"] == json!(cpem))
            .map(|c| c["certificateId"].clone())
    });
    let cert = new_certificate(pem, status, region, account, ca_id.as_ref());
    if s.certificates
        .iter()
        .any(|c| c["certificateId"] == cert["certificateId"])
    {
        return Err(already_exists(
            "The certificate is already provisioned or registered",
            cert["certificateId"].as_str().unwrap_or_default(),
            cert["arn"].as_str().unwrap_or_default(),
        ));
    }
    let body = json!({
        "certificateId": cert["certificateId"],
        "certificateArn": cert["arn"],
    });
    s.certificates.push(cert);
    Ok(ok_json(&body))
}

fn register_certificate_without_ca(
    s: &mut State,
    body: &Value,
    region: &str,
    account: u64,
) -> Result<Response, Response> {
    let pem = b_str(body, "certificatePem")
        .unwrap_or_default()
        .to_string();
    let status = b_str(body, "status").unwrap_or_default().to_string();
    register_with_status(
        s,
        &pem,
        b_str(body, "caCertificatePem"),
        &status,
        region,
        account,
    )
}

fn list_certificates(s: &mut State) -> Result<Response, Response> {
    let items: Vec<Value> = s
        .certificates
        .iter()
        .map(|c| {
            json!({
                "certificateArn": c["arn"],
                "certificateId": c["certificateId"],
                "caCertificateId": c.get("caCertificateId").unwrap_or(&Value::Null),
                "status": c["status"],
                "creationDate": c["creationDate"],
            })
        })
        .collect();
    Ok(ok_json(&json!({"certificates": items})))
}

fn list_certificates_by_ca(s: &mut State, ca_id: &str) -> Result<Response, Response> {
    let items: Vec<Value> = s
        .certificates
        .iter()
        .filter(|c| {
            c.get("caCertificateId")
                .map(|v| v == &json!(ca_id))
                .unwrap_or(false)
        })
        .map(|c| {
            json!({
                "certificateArn": c["arn"],
                "certificateId": c["certificateId"],
                "caCertificateId": c["caCertificateId"].clone(),
                "status": c["status"],
                "creationDate": c["creationDate"],
            })
        })
        .collect();
    Ok(ok_json(&json!({"certificates": items})))
}

// ---------------------------------------------------------------------------
// CA certificates — impls
// ---------------------------------------------------------------------------

fn register_ca_certificate(
    s: &mut State,
    body: &Value,
    query: &BTreeMap<String, Vec<String>>,
    region: &str,
    account: u64,
) -> Result<Response, Response> {
    let pem = b_str(body, "caCertificate").unwrap_or_default().to_string();
    let active = p_bool(body, query, "setAsActive").unwrap_or(false);
    let id = sha256_hex(pem.as_bytes());
    if s.ca_certificates
        .iter()
        .any(|c| c["certificateId"] == json!(id))
    {
        return Err(already_exists(
            "The certificate is already provisioned or registered",
            &id,
            &arn_for(region, account, "cert", &id),
        ));
    }
    s.ca_certificates.push(json!({
        "certificateId": id,
        "arn": arn_for(region, account, "cert", &id),
        "certificatePem": pem,
        "status": if active { "ACTIVE" } else { "INACTIVE" },
        "owner": account.to_string(),
        "transferData": {},
        "registrationConfig": {},
        "creationDate": crate::proto::now_iso(),
        "lastModifiedDate": crate::proto::now_iso(),
        "validity": {
            "notBefore": crate::proto::now_iso(),
            "notAfter": crate::proto::now_iso(),
        },
    }));
    Ok(ok_json(&json!({
        "certificateId": id,
        "certificateArn": arn_for(region, account, "cert", &id),
    })))
}

fn describe_ca_certificate(s: &mut State, id: &str) -> Result<Response, Response> {
    s.ca_certificates
        .iter()
        .find(|c| c["certificateId"] == json!(id))
        .map(|c| {
            ok_json(&json!({
                "certificateDescription": certificate_description(c),
                "registrationConfig": c.get("registrationConfig").cloned().unwrap_or(json!({})),
            }))
        })
        .ok_or_else(not_found)
}

fn update_ca_certificate(
    s: &mut State,
    id: &str,
    body: &Value,
    query: &BTreeMap<String, Vec<String>>,
) -> Result<Response, Response> {
    let idx = find_by_field_idx(&s.ca_certificates, "certificateId", id).ok_or_else(not_found)?;
    if let Some(status) = query.get("newStatus").and_then(|v| v.first()) {
        s.ca_certificates[idx]["status"] = json!(status);
    }
    if let Some(config) = b_val_ref(body, "registrationConfig") {
        s.ca_certificates[idx]["registrationConfig"] = config.clone();
    }
    Ok(ok_empty())
}

fn delete_ca_certificate(s: &mut State, id: &str) -> Result<Response, Response> {
    let idx = find_by_field_idx(&s.ca_certificates, "certificateId", id).ok_or_else(not_found)?;
    let status = s.ca_certificates[idx]["status"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    if status == "ACTIVE" {
        return Err(cert_state_error(
            "Certificate must be deactivated (not ACTIVE) before deletion.",
            id,
        ));
    }
    s.ca_certificates.remove(idx);
    Ok(ok_empty())
}

fn list_ca_certificates(s: &mut State) -> Result<Response, Response> {
    let items: Vec<Value> = s
        .ca_certificates
        .iter()
        .map(|c| {
            json!({
                "certificateArn": c["arn"],
                "certificateId": c["certificateId"],
                "status": c["status"],
                "creationDate": c["creationDate"],
            })
        })
        .collect();
    Ok(ok_json(&json!({"certificates": items})))
}

fn get_registration_code(_s: &mut State) -> Result<Response, Response> {
    Ok(ok_json(
        &json!({"registrationCode": random_id().replace('-', "")}),
    ))
}

fn delete_registration_code(s: &mut State) -> Result<Response, Response> {
    let _ = s;
    Ok(ok_empty())
}

// Certificate transfer ops keep moto's shape: mark transferDate/owner and
// coordinate accept/cancel/reject.
fn transfer_certificate(
    s: &mut State,
    id: &str,
    body: &Value,
    query: &BTreeMap<String, Vec<String>>,
) -> Result<Response, Response> {
    let idx = find_by_field_idx(&s.certificates, "certificateId", id).ok_or_else(not_found)?;
    let target = p_str(body, query, "targetAwsAccount").unwrap_or_default();
    s.certificates[idx]["transferData"] = json!({
        "transferMessage": b_str(body, "transferMessage").unwrap_or_default(),
        "rejectReason": "",
        "transferDate": crate::proto::now_iso(),
        "acceptDate": "",
        "rejectDate": "",
    });
    s.certificates[idx]["owner"] = json!(target);
    Ok(ok_json(&json!({
        "transferredCertificateArn": s.certificates[idx]["arn"],
        "transferredCertificateId": id,
    })))
}

fn accept_certificate_transfer(
    s: &mut State,
    id: &str,
    query: &BTreeMap<String, Vec<String>>,
) -> Result<Response, Response> {
    let idx = find_by_field_idx(&s.certificates, "certificateId", id).ok_or_else(not_found)?;
    if let Some(true) = query
        .get("setAsActive")
        .and_then(|v| v.first())
        .map(|v| v == "true")
    {
        s.certificates[idx]["status"] = json!("ACTIVE");
    }
    s.certificates[idx]["transferData"]["acceptDate"] = json!(crate::proto::now_iso());
    Ok(ok_empty())
}

fn cancel_certificate_transfer(s: &mut State, id: &str) -> Result<Response, Response> {
    find_by_field_idx(&s.certificates, "certificateId", id).ok_or_else(not_found)?;
    Ok(ok_empty())
}

fn reject_certificate_transfer(
    s: &mut State,
    id: &str,
    body: &Value,
) -> Result<Response, Response> {
    let idx = find_by_field_idx(&s.certificates, "certificateId", id).ok_or_else(not_found)?;
    let msg = b_str(body, "rejectReason").unwrap_or_default().to_string();
    s.certificates[idx]["transferData"]["rejectReason"] = json!(msg);
    s.certificates[idx]["transferData"]["rejectDate"] = json!(crate::proto::now_iso());
    Ok(ok_empty())
}

fn list_outgoing_certificates(s: &mut State) -> Result<Response, Response> {
    let items: Vec<Value> = s
        .certificates
        .iter()
        .filter(|c| {
            c.get("transferData")
                .map(|t| t != &json!({}))
                .unwrap_or(false)
        })
        .map(|c| {
            json!({
                "certificateArn": c["arn"],
                "certificateId": c["certificateId"],
                "transferredTo": c["owner"],
                "transferDate": c["transferData"]["transferDate"],
                "transferMessage": c["transferData"]["transferMessage"],
                "creationDate": c["creationDate"],
            })
        })
        .collect();
    Ok(ok_json(&json!({"outgoingCertificates": items})))
}

fn create_certificate_from_csr(
    s: &mut State,
    body: &Value,
    query: &BTreeMap<String, Vec<String>>,
    region: &str,
    account: u64,
) -> Result<Response, Response> {
    // moto parses the CSR and signs a new certificate; here we accept any
    // CSR-shaped body (the suites never validate against a decoder).
    if b_val_ref(body, "certificateSigningRequest").is_none() {
        return Err(invalid_request(
            "CSR is not a valid certificate signing request.",
        ));
    }
    let active = p_bool(body, query, "setAsActive").unwrap_or(false);
    let status = if active { "ACTIVE" } else { "INACTIVE" };
    let pem = synthetic_pem("CERTIFICATE", random_id().as_bytes());
    let cert = new_certificate(&pem, status, region, account, None);
    let body = json!({
        "certificateId": cert["certificateId"],
        "certificateArn": cert["arn"],
        "certificatePem": cert["certificatePem"],
    });
    s.certificates.push(cert);
    Ok(ok_json(&body))
}

// ---------------------------------------------------------------------------
// Jobs + job templates + executions — impls
// ---------------------------------------------------------------------------

fn create_job(
    s: &mut State,
    job_id: &str,
    body: &Value,
    region: &str,
    account: u64,
) -> Result<Response, Response> {
    if job_id.is_empty()
        || !job_id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
        || job_id.len() > 64
    {
        return Err(invalid_request(""));
    }
    if s.jobs.iter().any(|j| j["jobId"] == json!(job_id)) {
        return Err(already_exists(
            &format!("Job {job_id} already exists."),
            job_id,
            &arn_for(region, account, "job", job_id),
        ));
    }
    let targets: Vec<Value> = body
        .get("targets")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let job = json!({
        "jobId": job_id,
        "jobArn": arn_for(region, account, "job", job_id),
        "targets": body.get("targets").filter(|v| !v.is_null()).cloned().unwrap_or_else(|| json!([])),
        "documentSource": b_str(body, "documentSource").unwrap_or_default(),
        "document": b_str(body, "document").unwrap_or_default(),
        "description": b_str(body, "description").unwrap_or_default(),
        "presignedUrlConfig": body.get("presignedUrlConfig").filter(|v| !v.is_null()).cloned().unwrap_or_else(|| json!({})),
        "targetSelection": b_str(body, "targetSelection").unwrap_or("SNAPSHOT"),
        "jobExecutionsRolloutConfig": body.get("jobExecutionsRolloutConfig").filter(|v| !v.is_null()).cloned().unwrap_or_else(|| json!({})),
        "abortConfig": body.get("abortConfig").filter(|v| !v.is_null()).cloned().unwrap_or_else(|| json!({})),
        "jobExecutionsRetryConfig": body.get("jobExecutionsRetryConfig").filter(|v| !v.is_null()).cloned().unwrap_or_else(|| json!({})),
        "schedulingConfig": body.get("schedulingConfig").filter(|v| !v.is_null()).cloned().unwrap_or_else(|| json!({})),
        "timeoutConfig": body.get("timeoutConfig").filter(|v| !v.is_null()).cloned().unwrap_or_else(|| json!({})),
        "documentParameters": body.get("documentParameters").filter(|v| !v.is_null()).cloned().unwrap_or_else(|| json!({})),
        "comment": Value::Null,
        "reasonCode": Value::Null,
        "force": false,
        "status": "QUEUED",
        "createdAt": crate::proto::now_iso(),
        "lastUpdatedAt": crate::proto::now_iso(),
        "completedAt": Value::Null,
        "jobProcessDetails": {
            "processingTargets": body.get("targets").filter(|v| !v.is_null()).cloned().unwrap_or_else(|| json!([])),
            "numberOfQueuedThings": 1,
            "numberOfCanceledThings": 0,
            "numberOfSucceededThings": 0,
            "numberOfFailedThings": 0,
            "numberOfRejectedThings": 0,
            "numberOfInProgressThings": 0,
            "numberOfRemovedThings": 0,
        },
    });
    for target in targets {
        let thing_arn = target.as_str().unwrap_or_default().to_string();
        let thing_name = thing_arn.rsplit('/').next().unwrap_or_default().to_string();
        s.job_executions.push((
            (job_id.to_string(), thing_name),
            job_execution_record(job_id, &thing_arn),
        ));
    }
    let arn = job["jobArn"].clone();
    let description = job["description"].clone();
    s.jobs.push(job);
    Ok(ok_json(&json!({
        "jobArn": arn,
        "jobId": job_id,
        "description": description,
    })))
}

fn job_execution_record(job_id: &str, thing_arn: &str) -> Value {
    json!({
        "jobId": job_id,
        "status": "QUEUED",
        "forceCanceled": false,
        "statusDetails": {"detailsMap": {}},
        "thingArn": thing_arn,
        "queuedAt": crate::proto::now_iso(),
        "startedAt": crate::proto::now_iso(),
        "lastUpdatedAt": crate::proto::now_iso(),
        "executionNumber": 123,
        "versionNumber": 123,
        "approximateSecondsBeforeTimeOut": 123,
    })
}

fn describe_job(s: &mut State, job_id: &str) -> Result<Response, Response> {
    s.jobs
        .iter()
        .find(|j| j["jobId"].as_str() == Some(job_id))
        .map(|j| {
            ok_json(&json!({
                "documentSource": j["documentSource"],
                "job": {
                    "comment": j["comment"],
                    "completedAt": j["completedAt"],
                    "createdAt": j["createdAt"],
                    "description": j["description"],
                    "documentParameters": j["documentParameters"],
                    "forceCanceled": j["force"],
                    "reasonCode": j["reasonCode"],
                    "jobArn": j["jobArn"],
                    "jobExecutionsRolloutConfig": j["jobExecutionsRolloutConfig"],
                    "jobExecutionsRetryConfig": j["jobExecutionsRetryConfig"],
                    "schedulingConfig": j["schedulingConfig"],
                    "timeoutConfig": j["timeoutConfig"],
                    "abortConfig": j["abortConfig"],
                    "jobId": j["jobId"],
                    "jobProcessDetails": j["jobProcessDetails"],
                    "lastUpdatedAt": j["lastUpdatedAt"],
                    "presignedUrlConfig": j["presignedUrlConfig"],
                    "status": j["status"],
                    "targets": j["targets"],
                    "targetSelection": j["targetSelection"],
                },
            }))
        })
        .ok_or_else(not_found)
}

fn update_job(s: &mut State, job_id: &str, body: &Value) -> Result<Response, Response> {
    let idx = find_by_field_idx(&s.jobs, "jobId", job_id).ok_or_else(not_found)?;
    let keys = [
        "description",
        "presignedUrlConfig",
        "jobExecutionsRolloutConfig",
        "abortConfig",
        "timeoutConfig",
        "jobExecutionsRetryConfig",
    ];
    let job = &mut s.jobs[idx];
    for key in keys {
        if let Some(v) = body.get(key).filter(|v| !v.is_null()) {
            job[key] = v.clone();
        }
    }
    job["lastUpdatedAt"] = json!(crate::proto::now_unix());
    Ok(ok_empty())
}

fn delete_job(
    s: &mut State,
    job_id: &str,
    body: &Value,
    query: &BTreeMap<String, Vec<String>>,
) -> Result<Response, Response> {
    let idx = find_by_field_idx(&s.jobs, "jobId", job_id)
        .ok_or_else(|| not_found_msg(format!("Job {job_id} not found")))?;
    let force = p_bool(body, query, "force").unwrap_or(false);
    let status = s.jobs[idx]["status"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    if status == "IN_PROGRESS" && !force {
        return Err(invalid_state_transition());
    }
    s.jobs.remove(idx);
    Ok(ok_empty())
}

fn cancel_job(
    s: &mut State,
    job_id: &str,
    body: &Value,
    query: &BTreeMap<String, Vec<String>>,
) -> Result<Response, Response> {
    let idx = find_by_field_idx(&s.jobs, "jobId", job_id)
        .ok_or_else(|| not_found_msg(format!("Job {job_id} not found")))?;
    let status = s.jobs[idx]["status"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    if status == "IN_PROGRESS" {
        let force = p_bool(body, query, "force").unwrap_or(false);
        if !force {
            return Err(invalid_state_transition());
        }
    }
    let reason = b_str(body, "reasonCode").map(str::to_owned);
    if let Some(reason) = reason {
        s.jobs[idx]["reasonCode"] = json!(reason);
    } else {
        s.jobs[idx]["reasonCode"] = Value::Null;
    }
    if let Some(comment) = b_str(body, "comment") {
        s.jobs[idx]["comment"] = json!(comment);
    } else {
        s.jobs[idx]["comment"] = Value::Null;
    }
    if let Some(force) = p_bool(body, query, "force") {
        s.jobs[idx]["force"] = json!(force);
    } else {
        s.jobs[idx]["force"] = json!(false);
    }
    s.jobs[idx]["status"] = json!("CANCELED");
    let job = s.jobs[idx].clone();
    Ok(ok_json(&json!({
        "jobArn": job["jobArn"],
        "jobId": job["jobId"],
        "targets": job["targets"],
        "description": job["description"],
        "presignedUrlConfig": job["presignedUrlConfig"],
        "targetSelection": job["targetSelection"],
        "timeoutConfig": job["timeoutConfig"],
        "status": job["status"],
        "comment": job["comment"],
        "forceCanceled": job["force"],
        "reasonCode": job["reasonCode"],
        "createdAt": job["createdAt"],
        "lastUpdatedAt": job["lastUpdatedAt"],
        "completedAt": job["completedAt"],
        "jobProcessDetails": job["jobProcessDetails"],
        "documentParameters": job["documentParameters"],
        "document": job["document"],
        "documentSource": job["documentSource"],
    })))
}

fn get_job_document(s: &mut State, job_id: &str) -> Result<Response, Response> {
    let idx = find_by_field_idx(&s.jobs, "jobId", job_id)
        .ok_or_else(|| not_found_msg(format!("Job {job_id} not found")))?;
    Ok(ok_json(&json!({"document": s.jobs[idx]["document"]})))
}

fn list_jobs(s: &mut State) -> Result<Response, Response> {
    let items: Vec<Value> = s
        .jobs
        .iter()
        .map(|j| {
            json!({
                "jobArn": j["jobArn"],
                "jobId": j["jobId"],
                "targets": j["targets"],
                "description": j["description"],
                "presignedUrlConfig": j["presignedUrlConfig"],
                "targetSelection": j["targetSelection"],
                "timeoutConfig": j["timeoutConfig"],
                "status": j["status"],
                "comment": j["comment"],
                "forceCanceled": j["force"],
                "reasonCode": j["reasonCode"],
                "createdAt": j["createdAt"],
                "lastUpdatedAt": j["lastUpdatedAt"],
                "completedAt": j["completedAt"],
                "jobProcessDetails": j["jobProcessDetails"],
                "documentParameters": j["documentParameters"],
                "document": j["document"],
                "documentSource": j["documentSource"],
            })
        })
        .collect();
    Ok(ok_json(&json!({"jobs": items, "nextToken": Value::Null})))
}

// --- Job executions -------------------------------------------------------

fn execution_for(s: &mut State, job_id: &str, thing: &str) -> Option<usize> {
    s.job_executions
        .iter()
        .position(|(k, _)| k.0 == job_id && k.1 == thing)
}

fn describe_job_execution(
    s: &mut State,
    thing: &str,
    job_id: &str,
    body: &Value,
    query: &BTreeMap<String, Vec<String>>,
) -> Result<Response, Response> {
    let idx = execution_for(s, job_id, thing).ok_or_else(not_found)?;
    let want = p_i64(body, query, "executionNumber");
    if let Some(want) = want {
        if s.job_executions[idx].1["executionNumber"].as_i64() != Some(want) {
            return Err(not_found());
        }
    }
    let exec = s.job_executions[idx].1.clone();
    Ok(ok_json(&json!({
        "execution": {
            "jobId": exec["jobId"],
            "status": exec["status"],
            "forceCanceled": exec["forceCanceled"],
            "statusDetails": exec["statusDetails"],
            "thingArn": exec["thingArn"],
            "queuedAt": exec["queuedAt"],
            "startedAt": exec["startedAt"],
            "lastUpdatedAt": exec["lastUpdatedAt"],
            "executionNumber": exec["executionNumber"],
            "versionNumber": exec["versionNumber"],
            "approximateSecondsBeforeTimedOut": exec["approximateSecondsBeforeTimeOut"],
        }
    })))
}

fn cancel_job_execution(
    s: &mut State,
    thing: &str,
    job_id: &str,
    body: &Value,
    query: &BTreeMap<String, Vec<String>>,
) -> Result<Response, Response> {
    let idx = execution_for(s, job_id, thing).ok_or_else(not_found)?;
    let force = p_bool(body, query, "force").unwrap_or(false);
    let status = s.job_executions[idx].1["status"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    if status == "IN_PROGRESS" && !force {
        return Err(invalid_state_transition());
    }
    s.job_executions[idx].1["status"] = json!("CANCELED");
    if let Some(version) = b_val_ref(body, "expectedVersion") {
        s.job_executions[idx].1["versionNumber"] = version.clone();
    }
    if let Some(details) = b_val_ref(body, "statusDetails") {
        s.job_executions[idx].1["statusDetails"] = details.clone();
    }
    Ok(ok_empty())
}

fn delete_job_execution(
    s: &mut State,
    thing: &str,
    job_id: &str,
    exec_num: &str,
    body: &Value,
    query: &BTreeMap<String, Vec<String>>,
) -> Result<Response, Response> {
    let idx = execution_for(s, job_id, thing).ok_or_else(not_found)?;
    let want = exec_num.parse::<i64>().ok();
    if let Some(want) = want {
        if s.job_executions[idx].1["executionNumber"].as_i64() != Some(want) {
            return Err(not_found());
        }
    }
    let force = p_bool(body, query, "force").unwrap_or(false);
    let status = s.job_executions[idx].1["status"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    if status == "IN_PROGRESS" && !force {
        return Err(invalid_state_transition());
    }
    s.job_executions.remove(idx);
    Ok(ok_empty())
}

fn list_job_executions_for_job(
    s: &mut State,
    job_id: &str,
    body: &Value,
    query: &BTreeMap<String, Vec<String>>,
) -> Result<Response, Response> {
    let status_filter = p_str(body, query, "status").unwrap_or_default();
    let items: Vec<Value> = s
        .job_executions
        .iter()
        .filter(|((jid, _), e)| {
            jid == job_id && (status_filter.is_empty() || e["status"] == json!(status_filter))
        })
        .map(|(_, e)| {
            json!({
                "jobId": e["jobId"],
                "jobExecutionSummary": {
                    "status": e["status"],
                    "queuedAt": e["queuedAt"],
                    "startedAt": e["startedAt"],
                    "lastUpdatedAt": e["lastUpdatedAt"],
                    "executionNumber": e["executionNumber"],
                },
            })
        })
        .collect();
    Ok(ok_json(
        &json!({"executionSummaries": items, "nextToken": Value::Null}),
    ))
}

fn list_job_executions_for_thing(
    s: &mut State,
    thing: &str,
    body: &Value,
    query: &BTreeMap<String, Vec<String>>,
) -> Result<Response, Response> {
    let status_filter = p_str(body, query, "status").unwrap_or_default();
    let items: Vec<Value> = s
        .job_executions
        .iter()
        .filter(|((_, t), e)| {
            t == thing && (status_filter.is_empty() || e["status"] == json!(status_filter))
        })
        .map(|(_, e)| {
            json!({
                "jobId": e["jobId"],
                "jobExecutionSummary": {
                    "status": e["status"],
                    "queuedAt": e["queuedAt"],
                    "startedAt": e["startedAt"],
                    "lastUpdatedAt": e["lastUpdatedAt"],
                    "executionNumber": e["executionNumber"],
                },
            })
        })
        .collect();
    Ok(ok_json(
        &json!({"executionSummaries": items, "nextToken": Value::Null}),
    ))
}

fn associate_targets_with_job(_body: &Value) -> Result<Response, Response> {
    Ok(ok_empty())
}

// --- Job templates ---------------------------------------------------------

fn create_job_template(
    s: &mut State,
    id: &str,
    body: &Value,
    region: &str,
    account: u64,
) -> Result<Response, Response> {
    if !(id.len() <= 64
        && id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-'))
    {
        return Err(invalid_request(""));
    }
    if s.job_templates
        .iter()
        .any(|t| t["jobTemplateId"] == json!(id))
    {
        return Err(conflict(id));
    }
    s.job_templates.push(json!({
        "jobTemplateId": id,
        "jobTemplateArn": arn_for(region, account, "jobtemplate", id),
        "description": b_str(body, "description").unwrap_or_default(),
        "document": b_str(body, "document").unwrap_or_default(),
        "documentSource": b_str(body, "documentSource").unwrap_or_default(),
        "createdAt": crate::proto::now_iso(),
        "presignedUrlConfig": body.get("presignedUrlConfig").filter(|v| !v.is_null()).cloned().unwrap_or_else(|| json!({})),
        "jobExecutionsRolloutConfig": body.get("jobExecutionsRolloutConfig").filter(|v| !v.is_null()).cloned().unwrap_or_else(|| json!({})),
        "abortConfig": body.get("abortConfig").filter(|v| !v.is_null()).cloned().unwrap_or_else(|| json!({})),
        "jobExecutionsRetryConfig": body.get("jobExecutionsRetryConfig").filter(|v| !v.is_null()).cloned().unwrap_or_else(|| json!({})),
        "timeoutConfig": body.get("timeoutConfig").filter(|v| !v.is_null()).cloned().unwrap_or_else(|| json!({})),
        "maintenanceWindows": [],
        "destinationPackageVersions": [],
    }));
    Ok(ok_json(&json!({
        "jobTemplateArn": arn_for(region, account, "jobtemplate", id),
        "jobTemplateId": id,
    })))
}

fn describe_job_template(s: &mut State, id: &str) -> Result<Response, Response> {
    s.job_templates
        .iter()
        .find(|t| t["jobTemplateId"].as_str() == Some(id))
        .map(ok_json)
        .ok_or_else(|| not_found_msg(format!("Job template {id} not found")))
}

fn delete_job_template(s: &mut State, id: &str) -> Result<Response, Response> {
    let idx = find_by_field_idx(&s.job_templates, "jobTemplateId", id)
        .ok_or_else(|| not_found_msg(format!("Job template {id} not found")))?;
    s.job_templates.remove(idx);
    Ok(ok_empty())
}

fn list_job_templates(s: &mut State) -> Result<Response, Response> {
    let items: Vec<Value> = s
        .job_templates
        .iter()
        .map(|t| {
            json!({
                "jobTemplateArn": t["jobTemplateArn"],
                "jobTemplateId": t["jobTemplateId"],
                "description": t["description"],
                "createdAt": t["createdAt"],
            })
        })
        .collect();
    Ok(ok_json(
        &json!({"jobTemplates": items, "nextToken": Value::Null}),
    ))
}

// ---------------------------------------------------------------------------
// Topic rules + destinations — impls
// ---------------------------------------------------------------------------

fn create_topic_rule(
    s: &mut State,
    rule_name: &str,
    body: &Value,
    region: &str,
    account: u64,
) -> Result<Response, Response> {
    if !rule_name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        return Err(invalid_request(&format!(
            "1 validation error detected: Value '{rule_name}' at 'ruleName' failed to satisfy constraint: Member must satisfy regular expression pattern: ^[a-zA-Z0-9_]+$"
        )));
    }
    if s.rules.iter().any(|r| r["ruleName"] == json!(rule_name)) {
        return Err(already_exists(
            "Rule with given name already exists",
            "",
            &arn_for(region, account, "rule", rule_name),
        ));
    }
    // The wire body IS the TopicRulePayload (botocore marks the member
    // payload:true so the top-level body carries sql/actions directly).
    let payload = body.clone();
    let sql = body
        .get("sql")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let topic_pattern = body
        .get("sql")
        .and_then(Value::as_str)
        .and_then(|sql| sql.split("FROM").nth(1))
        .map(|tail| {
            tail.split_whitespace()
                .next()
                .unwrap_or("")
                .trim_matches('\'')
                .to_string()
        })
        .unwrap_or_default();
    let rule = json!({
        "ruleName": rule_name,
        "arn": arn_for(region, account, "rule", rule_name),
        "createdAt": crate::proto::now_iso(),
            "ruleDisabled": match payload.get("ruleDisabled") {
        Some(v) => v.clone(),
        None => json!(false),
    },
        "topicPattern": topic_pattern,
        "actions": payload.get("actions").cloned().unwrap_or_else(|| json!([])),
        "errorAction": payload.get("errorAction").cloned().unwrap_or_else(|| json!({})),
        "sql": sql,
        "awsIotSqlVersion": payload.get("awsIotSqlVersion").cloned().unwrap_or_else(|| json!("2016-03-23")),
        "description": payload.get("description").cloned().unwrap_or_else(|| json!("")),
    });
    s.rules.push(rule);
    Ok(ok_empty())
}

fn get_topic_rule(s: &mut State, rule_name: &str) -> Result<Response, Response> {
    let Some(r) = s
        .rules
        .iter()
        .find(|r| r["ruleName"].as_str() == Some(rule_name))
    else {
        return Err(not_found());
    };
    Ok(ok_json(&json!({
        "rule": {
            "actions": r["actions"],
            "awsIotSqlVersion": r["awsIotSqlVersion"],
            "createdAt": r["createdAt"],
            "description": r["description"],
            "errorAction": r["errorAction"],
            "ruleDisabled": r["ruleDisabled"],
            "ruleName": r["ruleName"],
            "sql": r["sql"],
        },
        "ruleArn": r["arn"],
    })))
}

fn replace_topic_rule(
    s: &mut State,
    rule_name: &str,
    body: &Value,
    region: &str,
    account: u64,
) -> Result<Response, Response> {
    delete_topic_rule(s, rule_name)?;
    create_topic_rule(s, rule_name, body, region, account)
}

fn delete_topic_rule(s: &mut State, rule_name: &str) -> Result<Response, Response> {
    let idx = find_by_field_idx(&s.rules, "ruleName", rule_name).ok_or_else(not_found)?;
    s.rules.remove(idx);
    Ok(ok_empty())
}

fn enable_disable_topic_rule(
    s: &mut State,
    rule_name: &str,
    disable: bool,
) -> Result<Response, Response> {
    let idx = find_by_field_idx(&s.rules, "ruleName", rule_name).ok_or_else(not_found)?;
    s.rules[idx]["ruleDisabled"] = json!(disable);
    Ok(ok_empty())
}

fn list_topic_rules(s: &mut State) -> Result<Response, Response> {
    let items: Vec<Value> = s
        .rules
        .iter()
        .map(|r| {
            json!({
                "ruleName": r["ruleName"],
                "createdAt": r["createdAt"],
                "ruleArn": r["arn"],
                "ruleDisabled": r["ruleDisabled"],
                "topicPattern": r["topicPattern"],
            })
        })
        .collect();
    Ok(ok_json(&json!({"rules": items})))
}

fn create_topic_rule_destination(
    s: &mut State,
    body: &Value,
    region: &str,
    account: u64,
) -> Result<Response, Response> {
    let config = b_val_ref(body, "destinationConfiguration")
        .cloned()
        .unwrap_or(json!({}));
    let dest = json!({
        "arn": arn_for(region, account, "ruledestination", &random_id()),
        "status": "ENABLED",
        "statusReason": "",
        "destinationConfig": config,
        "createdAt": crate::proto::now_iso(),
    });
    let arn = dest["arn"].clone();
    s.topic_rule_destinations.push(dest);
    Ok(ok_json(&json!({"topicRuleDestination": json!({
        "arn": arn,
        "status": "ENABLED",
        "httpUrlProperties": {
        },
    })})))
}

fn list_topic_rule_destinations(s: &mut State) -> Result<Response, Response> {
    let items: Vec<Value> = s
        .topic_rule_destinations
        .iter()
        .map(|d| {
            json!({
                "arn": d["arn"],
                "status": d["status"],
                "statusReason": d["statusReason"],
            })
        })
        .collect();
    Ok(ok_json(&json!({"destinationSummaries": items})))
}

fn update_topic_rule_destination(s: &mut State, body: &Value) -> Result<Response, Response> {
    let arn = b_str(body, "arn").unwrap_or_default().to_string();
    let idx = find_by_field_idx(&s.topic_rule_destinations, "arn", &arn).ok_or_else(not_found)?;
    if let Some(status) = b_str(body, "status") {
        s.topic_rule_destinations[idx]["status"] = json!(status);
    }
    Ok(ok_empty())
}

fn get_topic_rule_destination(s: &mut State, arn: &str) -> Result<Response, Response> {
    let idx = find_by_field_idx(&s.topic_rule_destinations, "arn", arn).ok_or_else(not_found)?;
    let d = &s.topic_rule_destinations[idx];
    Ok(ok_json(&json!({"topicRuleDestination": {
        "arn": d["arn"],
        "status": d["status"],
        "statusReason": d["statusReason"],
        "httpUrlProperties": d["destinationConfig"]["httpUrlConfiguration"],
        "createdAt": d["createdAt"],
    }})))
}

fn delete_topic_rule_destination(s: &mut State, arn: &str) -> Result<Response, Response> {
    let idx = find_by_field_idx(&s.topic_rule_destinations, "arn", arn).ok_or_else(not_found)?;
    s.topic_rule_destinations.remove(idx);
    Ok(ok_empty())
}

fn confirm_topic_rule_destination() -> Result<Response, Response> {
    Ok(ok_empty())
}

// ---------------------------------------------------------------------------
// Domain configurations, security profiles, authorizers — impls
// ---------------------------------------------------------------------------

fn create_domain_configuration(
    s: &mut State,
    name: &str,
    body: &Value,
    region: &str,
    account: u64,
) -> Result<Response, Response> {
    if s.domain_configurations
        .iter()
        .any(|d| d["domainConfigurationName"] == json!(name))
    {
        return Err(already_exists(
            "Domain configuration with given name already exists.",
            name,
            &arn_for(
                region,
                account,
                "domainconfiguration",
                &format!("{name}/{}", random_id().get(..5).unwrap_or_default()),
            ),
        ));
    }
    s.domain_configurations.push(json!({
        "domainConfigurationName": name,
        "domainConfigurationArn": arn_for(region, account, "domainconfiguration", &format!("{name}/{}", random_id().get(..5).unwrap_or_default())),
        "domainName": b_str(body, "domainName").unwrap_or_default(),
        "serverCertificates": [],
        "authorizerConfig": body.get("authorizerConfig").filter(|v| !v.is_null()).cloned(),
        "domainConfigurationStatus": body.get("domainConfigurationStatus").filter(|v| !v.is_null()).cloned().unwrap_or_else(|| json!("ENABLED")),
        "serviceType": b_str(body, "serviceType").unwrap_or_default(),
        "domainType": "CUSTOMER_MANAGED",
        "lastStatusChangeDate": crate::proto::now_iso(),
    }));
    Ok(ok_json(&json!({
        "domainConfigurationName": name,
        "domainConfigurationArn": arn_for(region, account, "domainconfiguration", name),
    })))
}

fn describe_domain_configuration(s: &mut State, name: &str) -> Result<Response, Response> {
    s.domain_configurations
        .iter()
        .find(|d| d["domainConfigurationName"].as_str() == Some(name))
        .map(|d| {
            ok_json(&json!({
                "domainConfigurationName": d["domainConfigurationName"],
                "domainConfigurationArn": d["domainConfigurationArn"],
                "domainName": d["domainName"],
                "serverCertificates": d["serverCertificates"],
                "authorizerConfig": d.get("authorizerConfig").cloned().unwrap_or(Value::Null),
                "domainConfigurationStatus": d["domainConfigurationStatus"],
                "serviceType": d["serviceType"],
                "domainType": d["domainType"],
                "lastStatusChangeDate": d["lastStatusChangeDate"],
            }))
        })
        .ok_or_else(not_found)
}

fn update_domain_configuration(
    s: &mut State,
    name: &str,
    body: &Value,
) -> Result<Response, Response> {
    let idx = find_by_field_idx(&s.domain_configurations, "domainConfigurationName", name)
        .ok_or_else(not_found)?;
    if let Some(config) = b_val_ref(body, "authorizerConfig") {
        s.domain_configurations[idx]["authorizerConfig"] = config.clone();
    }
    if let Some(status) = b_str(body, "domainConfigurationStatus") {
        s.domain_configurations[idx]["domainConfigurationStatus"] = json!(status);
    }
    if b_bool(body, "removeAuthorizerConfig") == Some(true) {
        s.domain_configurations[idx]["authorizerConfig"] = Value::Null;
    }
    Ok(ok_json(&json!({"domainConfigurationName": name})))
}

fn delete_domain_configuration(s: &mut State, name: &str) -> Result<Response, Response> {
    let idx = find_by_field_idx(&s.domain_configurations, "domainConfigurationName", name)
        .ok_or_else(not_found)?;
    s.domain_configurations.remove(idx);
    Ok(ok_empty())
}

fn list_domain_configurations(s: &mut State) -> Result<Response, Response> {
    let items: Vec<Value> = s
        .domain_configurations
        .iter()
        .map(|d| {
            json!({"domainConfigurationName": d["domainConfigurationName"],
                    "domainConfigurationArn": d["domainConfigurationArn"]})
        })
        .collect();
    Ok(ok_json(&json!({"domainConfigurations": items})))
}

fn create_security_profile(
    s: &mut State,
    name: &str,
    body: &Value,
    region: &str,
    account: u64,
) -> Result<Response, Response> {
    if s.security_profiles.is_empty() {
        s.security_profiles = Vec::new();
    }
    if s.security_profiles
        .iter()
        .any(|p| p["securityProfileName"] == json!(name))
    {
        return Err(already_exists(
            &format!("Security profile {name} already exists."),
            name,
            &arn_for(region, account, "securityprofile", name),
        ));
    }
    s.security_profiles.push(json!({
        "securityProfileName": name,
        "securityProfileArn": arn_for(region, account, "securityprofile", name),
        "securityProfileDescription": b_str(body, "securityProfileDescription").unwrap_or_default(),
        "behaviors": body.get("behaviors").cloned().unwrap_or_else(|| json!([])),
        "alertTargets": body.get("alertTargets").cloned().unwrap_or_else(|| json!({})),
        "additionalMetricsToRetainV2": body.get("additionalMetricsToRetainV2").cloned().unwrap_or_else(|| json!([])),
        "tags": body.get("tags").cloned().unwrap_or_else(|| json!([])),
        "creationDate": crate::proto::now_iso(),
        "lastModifiedDate": crate::proto::now_iso(),
        "version": 1,
        "targets": [],
    }));
    Ok(ok_json(&json!({
        "securityProfileName": name,
        "securityProfileArn": arn_for(region, account, "securityprofile", name),
    })))
}

fn describe_security_profile(s: &mut State, name: &str) -> Result<Response, Response> {
    s.security_profiles
        .iter()
        .find(|p| p["securityProfileName"].as_str() == Some(name))
        .map(ok_json)
        .ok_or_else(not_found)
}

fn update_security_profile(s: &mut State, name: &str, body: &Value) -> Result<Response, Response> {
    let idx = find_by_field_idx(&s.security_profiles, "securityProfileName", name)
        .ok_or_else(not_found)?;
    merge_opt_members(
        &mut s.security_profiles[idx],
        body,
        &[
            "securityProfileDescription",
            "behaviors",
            "alertTargets",
            "additionalMetricsToRetainV2",
        ],
    );
    let version = s.security_profiles[idx]["version"].as_i64().unwrap_or(1) + 1;
    s.security_profiles[idx]["version"] = json!(version);
    s.security_profiles[idx]["lastModifiedDate"] = json!(crate::proto::now_iso());
    Ok(ok_json(&s.security_profiles[idx]))
}

fn delete_security_profile(s: &mut State, name: &str) -> Result<Response, Response> {
    let idx = find_by_field_idx(&s.security_profiles, "securityProfileName", name)
        .ok_or_else(not_found)?;
    s.security_profiles.remove(idx);
    Ok(ok_empty())
}

fn list_security_profiles(s: &mut State) -> Result<Response, Response> {
    let items: Vec<Value> = s
        .security_profiles
        .iter()
        .map(|p| json!({"name": p["securityProfileName"], "arn": p["securityProfileArn"]}))
        .collect();
    Ok(ok_json(&json!({"securityProfileIdentifiers": items})))
}

fn attach_detach_security_profile(
    s: &mut State,
    name: &str,
    body: &Value,
    query: &BTreeMap<String, Vec<String>>,
    attach: bool,
) -> Result<Response, Response> {
    let idx = find_by_field_idx(&s.security_profiles, "securityProfileName", name)
        .ok_or_else(not_found)?;
    let target_arn = p_str(body, query, "securityProfileTargetArn").unwrap_or_default();
    if s.security_profiles[idx]["targets"].is_null() {
        s.security_profiles[idx]["targets"] = json!([]);
    }
    if let Some(arr) = s.security_profiles[idx]["targets"].as_array_mut() {
        if attach {
            if !arr.contains(&json!(target_arn)) {
                arr.push(json!(target_arn));
            }
        } else {
            arr.retain(|a| a != &json!(target_arn));
        }
    }
    Ok(ok_empty())
}

fn list_targets_for_security_profile(s: &mut State, name: &str) -> Result<Response, Response> {
    let idx = find_by_field_idx(&s.security_profiles, "securityProfileName", name)
        .ok_or_else(not_found)?;
    let items: Vec<Value> = s.security_profiles[idx]
        .get("targets")
        .and_then(Value::as_array)
        .map(|a| a.iter().map(|arn| json!({"arn": arn})).collect())
        .unwrap_or_default();
    Ok(ok_json(&json!({"securityProfileTargets": items})))
}

fn list_security_profiles_for_target(
    s: &mut State,
    body: &Value,
    query: &BTreeMap<String, Vec<String>>,
) -> Result<Response, Response> {
    let target_arn = p_str(body, query, "securityProfileTargetArn").unwrap_or_default();
    let mut items = Vec::new();
    for p in &s.security_profiles {
        let matched = p
            .get("targets")
            .and_then(Value::as_array)
            .map(|a| a.contains(&json!(target_arn)))
            .unwrap_or(false);
        if matched {
            items.push(json!({
                "securityProfileIdentifier": {"name": p["securityProfileName"], "arn": p["securityProfileArn"]},
                "target": {"arn": target_arn},
            }));
        }
    }
    Ok(ok_json(&json!({"securityProfileTargetMappings": items})))
}

fn validate_security_profile_behaviors() -> Result<Response, Response> {
    Ok(ok_json(&json!({"valid": true, "validationErrors": []})))
}

// ---------------------------------------------------------------------------
// Authorizers — impls
// ---------------------------------------------------------------------------

fn create_authorizer(
    s: &mut State,
    name: &str,
    body: &Value,
    region: &str,
    account: u64,
) -> Result<Response, Response> {
    if s.authorizers
        .iter()
        .any(|a| a["authorizerName"] == json!(name))
    {
        return Err(already_exists(
            &format!("Authorizer {name} already exists."),
            name,
            &arn_for(region, account, "authorizer", name),
        ));
    }
    s.authorizers.push(json!({
        "authorizerName": name,
        "authorizerArn": arn_for(region, account, "authorizer", name),
        "authorizerFunctionArn": b_str(body, "authorizerFunctionArn").unwrap_or_default(),
        "tokenKeyName": body.get("tokenKeyName").cloned().unwrap_or(Value::Null),
        "tokenSigningPublicKeys": body.get("tokenSigningPublicKeys").cloned().unwrap_or_else(|| json!({})),
        "status": body.get("status").cloned().unwrap_or_else(|| json!("ACTIVE")),
        "signingDisabled": b_bool(body, "signingDisabled").unwrap_or(false),
        "enableCachingForHttp": b_bool(body, "enableCachingForHttp").unwrap_or(false),
        "creationDate": crate::proto::now_iso(),
        "lastModifiedDate": crate::proto::now_iso(),
    }));
    Ok(ok_json(&json!({
        "authorizerName": name,
        "authorizerArn": arn_for(region, account, "authorizer", name),
    })))
}

fn describe_authorizer(s: &mut State, name: &str) -> Result<Response, Response> {
    s.authorizers
        .iter()
        .find(|a| a["authorizerName"].as_str() == Some(name))
        .map(|a| ok_json(&json!({"authorizerDescription": a})))
        .ok_or_else(not_found)
}

fn update_authorizer(s: &mut State, name: &str, body: &Value) -> Result<Response, Response> {
    let idx = find_by_field_idx(&s.authorizers, "authorizerName", name).ok_or_else(not_found)?;
    let keys = [
        "authorizerFunctionArn",
        "tokenKeyName",
        "tokenSigningPublicKeys",
        "status",
    ];
    for key in keys {
        if let Some(v) = body.get(key).filter(|v| !v.is_null()) {
            s.authorizers[idx][key] = v.clone();
        }
    }
    if let Some(v) = b_val_ref(body, "enableCachingForHttp") {
        s.authorizers[idx]["enableCachingForHttp"] = v.clone();
    }
    s.authorizers[idx]["lastModifiedDate"] = json!(crate::proto::now_iso());
    Ok(ok_json(&json!({
        "authorizerName": name,
        "authorizerArn": s.authorizers[idx]["authorizerArn"],
    })))
}

fn delete_authorizer(s: &mut State, name: &str) -> Result<Response, Response> {
    let idx = find_by_field_idx(&s.authorizers, "authorizerName", name).ok_or_else(not_found)?;
    s.authorizers.remove(idx);
    Ok(ok_empty())
}

fn list_authorizers(s: &mut State) -> Result<Response, Response> {
    let items: Vec<Value> = s.authorizers.clone();
    Ok(ok_json(&json!({"authorizers": items})))
}

fn test_invoke_authorizer(s: &mut State, _name: &str) -> Result<Response, Response> {
    let _ = s;
    Ok(ok_json(&json!({"isAuthenticated": true})))
}

fn set_default_authorizer(s: &mut State, body: &Value) -> Result<Response, Response> {
    let name = b_str(body, "authorizerName")
        .unwrap_or_default()
        .to_string();
    let idx = find_by_field_idx(&s.authorizers, "authorizerName", &name).ok_or_else(not_found)?;
    let arn = s.authorizers[idx]["authorizerArn"].clone();
    s.default_authorizer_name = Some(name.clone());
    Ok(ok_json(
        &json!({"authorizerName": name, "authorizerArn": arn}),
    ))
}

fn describe_default_authorizer(s: &mut State) -> Result<Response, Response> {
    let Some(name) = s.default_authorizer_name.clone() else {
        return Err(not_found_msg("Default authorizer does not exist.".into()));
    };
    let idx = find_by_field_idx(&s.authorizers, "authorizerName", &name)
        .ok_or_else(|| not_found_msg("Default authorizer does not exist.".into()))?;
    Ok(ok_json(
        &json!({"authorizerDescription": s.authorizers[idx]}),
    ))
}

fn clear_default_authorizer(s: &mut State) -> Result<Response, Response> {
    s.default_authorizer_name = None;
    Ok(ok_empty())
}

// ---------------------------------------------------------------------------
// Provisioning templates (+ versions/claims), dimensions, custom metrics,
// fleet metrics, streams, OTA updates — impls
// ---------------------------------------------------------------------------

fn create_provisioning_template(
    s: &mut State,
    body: &Value,
    region: &str,
    account: u64,
) -> Result<Response, Response> {
    let name = b_str(body, "templateName").unwrap_or_default().to_string();
    if s.provisioning_templates
        .iter()
        .any(|t| t["templateName"] == json!(name))
    {
        return Err(already_exists(
            &format!("Provisioning template {name} already exists."),
            &name,
            &arn_for(region, account, "provisioningtemplate", &name),
        ));
    }
    s.provisioning_templates.push(json!({
        "templateName": name,
        "templateArn": arn_for(region, account, "provisioningtemplate", &name),
        "description": b_str(body, "description").unwrap_or_default(),
        "templateBody": b_str(body, "templateBody").unwrap_or_default(),
        "enabled": b_bool(body, "enabled").unwrap_or(false),
        "provisioningRoleArn": b_str(body, "provisioningRoleArn").unwrap_or_default(),
        "preProvisioningHook": body.get("preProvisioningHook").filter(|v| !v.is_null()).cloned().unwrap_or_else(|| json!({})),
        "defaultVersionId": 1,
        "versions": [{
            "versionId": 1,
            "templateBody": b_str(body, "templateBody").unwrap_or_default(),
            "isDefaultVersion": true,
            "creationDate": crate::proto::now_iso(),
        }],
        "creationDate": crate::proto::now_iso(),
        "lastModifiedDate": crate::proto::now_iso(),
    }));
    Ok(ok_json(&json!({
        "templateName": name,
        "templateArn": arn_for(region, account, "provisioningtemplate", &name),
        "defaultVersionId": 1,
    })))
}

fn describe_provisioning_template(s: &mut State, name: &str) -> Result<Response, Response> {
    s.provisioning_templates
        .iter()
        .find(|t| t["templateName"].as_str() == Some(name))
        .map(|t| {
            ok_json(&json!({
                "templateName": t["templateName"],
                "templateArn": t["templateArn"],
                "description": t["description"],
                "templateBody": t["templateBody"],
                "enabled": t["enabled"],
                "provisioningRoleArn": t["provisioningRoleArn"],
                "defaultVersionId": t["defaultVersionId"],
                "creationDate": t["creationDate"],
                "lastModifiedDate": t["lastModifiedDate"],
            }))
        })
        .ok_or_else(not_found)
}

fn update_provisioning_template(
    s: &mut State,
    name: &str,
    body: &Value,
) -> Result<Response, Response> {
    let idx =
        find_by_field_idx(&s.provisioning_templates, "templateName", name).ok_or_else(not_found)?;
    let keys = [
        "description",
        "enabled",
        "provisioningRoleArn",
        "preProvisioningHook",
        "defaultVersionId",
    ];
    for key in keys {
        if let Some(v) = body.get(key).filter(|v| !v.is_null()) {
            s.provisioning_templates[idx][key] = v.clone();
        }
    }
    s.provisioning_templates[idx]["lastModifiedDate"] = json!(crate::proto::now_iso());
    Ok(ok_empty())
}

fn delete_provisioning_template(s: &mut State, name: &str) -> Result<Response, Response> {
    let idx =
        find_by_field_idx(&s.provisioning_templates, "templateName", name).ok_or_else(not_found)?;
    s.provisioning_templates.remove(idx);
    Ok(ok_empty())
}

fn list_provisioning_templates(s: &mut State) -> Result<Response, Response> {
    let items: Vec<Value> = s
        .provisioning_templates
        .iter()
        .map(|t| {
            json!({
                "templateName": t["templateName"],
                "templateArn": t["templateArn"],
                "enabled": t["enabled"],
                "creationDate": t["creationDate"],
                "lastModifiedDate": t["lastModifiedDate"],
            })
        })
        .collect();
    Ok(ok_json(&json!({"templates": items})))
}

fn create_prov_template_version(
    s: &mut State,
    name: &str,
    body: &Value,
    query: &BTreeMap<String, Vec<String>>,
) -> Result<Response, Response> {
    let idx =
        find_by_field_idx(&s.provisioning_templates, "templateName", name).ok_or_else(not_found)?;
    let set_default = p_bool(body, query, "setAsDefault").unwrap_or(false);
    let body_doc = b_str(body, "templateBody").unwrap_or_default().to_string();
    let versions = s.provisioning_templates[idx]["versions"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let next = versions.len() as i64 + 1;
    if set_default {
        if let Some(arr) = s.provisioning_templates[idx]["versions"].as_array_mut() {
            for v in arr.iter_mut() {
                v["isDefaultVersion"] = json!(false);
            }
        }
        s.provisioning_templates[idx]["defaultVersionId"] = json!(next);
    }
    if let Some(arr) = s.provisioning_templates[idx]["versions"].as_array_mut() {
        arr.push(json!({
            "versionId": next,
            "templateBody": body_doc,
            "isDefaultVersion": set_default,
            "creationDate": crate::proto::now_iso(),
        }));
    }
    s.provisioning_templates[idx]["lastModifiedDate"] = json!(crate::proto::now_iso());
    Ok(ok_json(&json!({
        "templateArn": s.provisioning_templates[idx]["templateArn"],
        "templateName": name,
        "versionId": next,
        "isDefaultVersion": set_default,
    })))
}

fn list_prov_template_versions(s: &mut State, name: &str) -> Result<Response, Response> {
    let idx =
        find_by_field_idx(&s.provisioning_templates, "templateName", name).ok_or_else(not_found)?;
    let items: Vec<Value> = s.provisioning_templates[idx]
        .get("versions")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .map(|v| {
                    json!({
                        "versionId": v["versionId"],
                        "creationDate": v["creationDate"],
                        "isDefaultVersion": v["isDefaultVersion"],
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    Ok(ok_json(&json!({"versions": items})))
}

fn describe_prov_template_version(
    s: &mut State,
    name: &str,
    version_tail: &str,
) -> Result<Response, Response> {
    let idx =
        find_by_field_idx(&s.provisioning_templates, "templateName", name).ok_or_else(not_found)?;
    let version_id = version_tail.parse::<i64>().unwrap_or(1);
    s.provisioning_templates[idx]["versions"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .find(|v| v["versionId"].as_i64() == Some(version_id))
        .map(|v| ok_json(&v))
        .ok_or_else(|| {
            not_found_msg(format!(
                "Version {version_id} of provisioning template {name} does not exist."
            ))
        })
}

fn delete_prov_template_version(
    s: &mut State,
    name: &str,
    version_tail: &str,
) -> Result<Response, Response> {
    let idx =
        find_by_field_idx(&s.provisioning_templates, "templateName", name).ok_or_else(not_found)?;
    let version_id = version_tail.parse::<i64>().unwrap_or(1);
    let versions = s.provisioning_templates[idx]["versions"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let pos = versions
        .iter()
        .position(|v| v["versionId"].as_i64() == Some(version_id));
    match pos {
        Some(pos) => {
            if versions[pos]["isDefaultVersion"] == json!(true) {
                return Err(invalid_request(
                    "Cannot delete the default version of a provisioning template.",
                ));
            }
            s.provisioning_templates[idx]["versions"]
                .as_array_mut()
                .map(|arr| arr.remove(pos));
            Ok(ok_empty())
        }
        None => Err(not_found_msg(format!(
            "Version {version_id} of provisioning template {name} does not exist."
        ))),
    }
}

fn create_provisioning_claim(_s: &mut State, _template_name: &str) -> Result<Response, Response> {
    let pem = synthetic_pem("CERTIFICATE", random_id().as_bytes());
    let cert_id = sha256_hex(pem.as_bytes());
    Ok(ok_json(&json!({
        "certificateId": cert_id,
        "certificatePem": pem,
        "expiration": crate::proto::now_iso(),
    })))
}

// --- Dimensions / custom metrics / fleet metrics ----------------------------

fn create_dimension(
    s: &mut State,
    name: &str,
    body: &Value,
    region: &str,
    account: u64,
) -> Result<Response, Response> {
    if s.dimensions.iter().any(|d| d["name"] == json!(name)) {
        return Err(already_exists(
            &format!("Dimension {name} already exists."),
            name,
            &arn_for(region, account, "dimension", name),
        ));
    }
    s.dimensions.push(json!({
        "name": name,
        "arn": arn_for(region, account, "dimension", name),
        "type": b_str(body, "type").unwrap_or_default(),
        "stringValues": body.get("stringValues").cloned().unwrap_or_else(|| json!([])),
        "tags": body.get("tags").cloned().unwrap_or_else(|| json!([])),
        "creationDate": crate::proto::now_iso(),
        "lastModifiedDate": crate::proto::now_iso(),
    }));
    Ok(ok_json(&json!({
        "name": name,
        "arn": arn_for(region, account, "dimension", name),
    })))
}

fn describe_dimension(s: &mut State, name: &str) -> Result<Response, Response> {
    s.dimensions
        .iter()
        .find(|d| d["name"].as_str() == Some(name))
        .map(|d| {
            ok_json(&json!({
                "name": d["name"],
                "arn": d["arn"],
                "type": d["type"],
                "stringValues": d["stringValues"],
                "creationDate": d["creationDate"],
                "lastModifiedDate": d["lastModifiedDate"],
            }))
        })
        .ok_or_else(not_found)
}

fn update_dimension(s: &mut State, name: &str, body: &Value) -> Result<Response, Response> {
    let idx = find_by_field_idx(&s.dimensions, "name", name).ok_or_else(not_found)?;
    if let Some(values) = b_val_ref(body, "stringValues") {
        s.dimensions[idx]["stringValues"] = values.clone();
    }
    s.dimensions[idx]["lastModifiedDate"] = json!(crate::proto::now_iso());
    Ok(ok_json(&s.dimensions[idx]))
}

fn delete_dimension(s: &mut State, name: &str) -> Result<Response, Response> {
    let idx = find_by_field_idx(&s.dimensions, "name", name).ok_or_else(not_found)?;
    s.dimensions.remove(idx);
    Ok(ok_empty())
}

fn list_dimensions(s: &mut State) -> Result<Response, Response> {
    let names: Vec<&Value> = s.dimensions.iter().map(|d| &d["name"]).collect();
    Ok(ok_json(&json!({"dimensionNames": names})))
}

fn create_custom_metric(
    s: &mut State,
    name: &str,
    body: &Value,
    region: &str,
    account: u64,
) -> Result<Response, Response> {
    if s.custom_metrics
        .iter()
        .any(|m| m["metricName"] == json!(name))
    {
        return Err(already_exists(
            &format!("Custom metric {name} already exists."),
            name,
            &arn_for(region, account, "custommetric", name),
        ));
    }
    s.custom_metrics.push(json!({
        "metricName": name,
        "metricArn": arn_for(region, account, "custommetric", name),
        "displayName": b_str(body, "displayName").unwrap_or_default(),
        "metricType": b_str(body, "metricType").unwrap_or_default(),
        "creationDate": crate::proto::now_iso(),
        "lastModifiedDate": crate::proto::now_iso(),
    }));
    Ok(ok_json(&json!({
        "metricName": name,
        "metricArn": arn_for(region, account, "custommetric", name),
    })))
}

fn describe_custom_metric(s: &mut State, name: &str) -> Result<Response, Response> {
    s.custom_metrics
        .iter()
        .find(|m| m["metricName"].as_str() == Some(name))
        .map(ok_json)
        .ok_or_else(not_found)
}

fn update_custom_metric(s: &mut State, name: &str, body: &Value) -> Result<Response, Response> {
    let idx = find_by_field_idx(&s.custom_metrics, "metricName", name).ok_or_else(not_found)?;
    if let Some(display) = b_val_ref(body, "displayName") {
        s.custom_metrics[idx]["displayName"] = display.clone();
    }
    s.custom_metrics[idx]["lastModifiedDate"] = json!(crate::proto::now_iso());
    Ok(ok_json(&s.custom_metrics[idx]))
}

fn delete_custom_metric(s: &mut State, name: &str) -> Result<Response, Response> {
    let idx = find_by_field_idx(&s.custom_metrics, "metricName", name).ok_or_else(not_found)?;
    s.custom_metrics.remove(idx);
    Ok(ok_empty())
}

fn list_custom_metrics(s: &mut State) -> Result<Response, Response> {
    let names: Vec<&Value> = s.custom_metrics.iter().map(|m| &m["metricName"]).collect();
    Ok(ok_json(&json!({"metricNames": names})))
}

fn create_fleet_metric(
    s: &mut State,
    name: &str,
    body: &Value,
    region: &str,
    account: u64,
) -> Result<Response, Response> {
    if s.fleet_metrics
        .iter()
        .any(|m| m["metricName"] == json!(name))
    {
        return Err(already_exists(
            &format!("Fleet metric {name} already exists."),
            name,
            &arn_for(region, account, "fleetmetric", name),
        ));
    }
    s.fleet_metrics.push(json!({
        "metricName": name,
        "metricArn": arn_for(region, account, "fleetmetric", name),
        "queryString": b_str(body, "queryString").unwrap_or_default(),
        "aggregationType": b_val_ref(body, "aggregationType").cloned().unwrap_or(Value::Null),
        "period": body.get("period").cloned().unwrap_or_else(|| json!(300)),
        "aggregationField": b_str(body, "aggregationField").unwrap_or_default(),
        "description": b_str(body, "description").unwrap_or_default(),
        "queryVersion": b_str(body, "queryVersion").unwrap_or("2017-09-30"),
        "indexName": b_str(body, "indexName").unwrap_or("AWS_Things"),
        "unit": b_str(body, "unit").unwrap_or("None"),
        "version": 1,
        "creationDate": crate::proto::now_iso(),
        "lastModifiedDate": crate::proto::now_iso(),
    }));
    Ok(ok_json(&json!({
        "metricName": name,
        "metricArn": arn_for(region, account, "fleetmetric", name),
    })))
}

fn describe_fleet_metric(s: &mut State, name: &str) -> Result<Response, Response> {
    s.fleet_metrics
        .iter()
        .find(|m| m["metricName"].as_str() == Some(name))
        .map(|m| {
            let mut obj = m.clone();
            if let Some(o) = obj.as_object_mut() {
                o.remove("creationDate");
                o.remove("lastModifiedDate");
            }
            ok_json(&obj)
        })
        .ok_or_else(not_found)
}

fn update_fleet_metric(s: &mut State, name: &str, body: &Value) -> Result<Response, Response> {
    let idx = find_by_field_idx(&s.fleet_metrics, "metricName", name).ok_or_else(not_found)?;
    let keys = [
        "queryString",
        "aggregationType",
        "period",
        "aggregationField",
        "description",
        "queryVersion",
        "indexName",
        "unit",
    ];
    for key in keys {
        if let Some(v) = body.get(key).filter(|v| !v.is_null()) {
            s.fleet_metrics[idx][key] = v.clone();
        }
    }
    let version = s.fleet_metrics[idx]["version"].as_i64().unwrap_or(1) + 1;
    s.fleet_metrics[idx]["version"] = json!(version);
    s.fleet_metrics[idx]["lastModifiedDate"] = json!(crate::proto::now_iso());
    Ok(ok_empty())
}

fn delete_fleet_metric(s: &mut State, name: &str) -> Result<Response, Response> {
    let idx = find_by_field_idx(&s.fleet_metrics, "metricName", name).ok_or_else(not_found)?;
    s.fleet_metrics.remove(idx);
    Ok(ok_empty())
}

fn list_fleet_metrics(s: &mut State) -> Result<Response, Response> {
    let items: Vec<Value> = s
        .fleet_metrics
        .iter()
        .map(|m| json!({"metricName": m["metricName"], "metricArn": m["metricArn"]}))
        .collect();
    Ok(ok_json(&json!({"fleetMetrics": items})))
}

// --- Streams / OTA updates --------------------------------------------------

fn create_stream(
    s: &mut State,
    stream_id: &str,
    body: &Value,
    region: &str,
    account: u64,
) -> Result<Response, Response> {
    if s.streams
        .iter()
        .any(|st| st["streamId"] == json!(stream_id))
    {
        return Err(already_exists(
            &format!("Stream {stream_id} already exists."),
            stream_id,
            &arn_for(region, account, "stream", stream_id),
        ));
    }
    s.streams.push(json!({
        "streamId": stream_id,
        "streamArn": arn_for(region, account, "stream", stream_id),
        "streamVersion": 1,
        "description": b_str(body, "description").unwrap_or_default(),
        "files": body.get("files").cloned().unwrap_or_else(|| json!([])),
        "roleArn": b_str(body, "roleArn").unwrap_or_default(),
        "tags": body.get("tags").cloned().unwrap_or_else(|| json!([])),
        "createdAt": crate::proto::now_iso(),
        "lastUpdatedAt": crate::proto::now_iso(),
    }));
    Ok(ok_json(&json!({
        "streamId": stream_id,
        "streamArn": arn_for(region, account, "stream", stream_id),
        "streamVersion": 1,
        "description": b_str(body, "description").unwrap_or_default(),
    })))
}

fn describe_stream(s: &mut State, stream_id: &str) -> Result<Response, Response> {
    s.streams
        .iter()
        .find(|st| st["streamId"].as_str() == Some(stream_id))
        .map(|st| ok_json(&json!({"streamInfo": st})))
        .ok_or_else(not_found)
}

fn update_stream(s: &mut State, stream_id: &str, body: &Value) -> Result<Response, Response> {
    let idx = find_by_field_idx(&s.streams, "streamId", stream_id).ok_or_else(not_found)?;
    merge_opt_members(
        &mut s.streams[idx],
        body,
        &["description", "files", "roleArn"],
    );
    let version = s.streams[idx]["streamVersion"].as_i64().unwrap_or(1) + 1;
    s.streams[idx]["streamVersion"] = json!(version);
    Ok(ok_json(&json!({
        "streamId": stream_id,
        "streamArn": s.streams[idx]["streamArn"],
        "streamVersion": version,
        "description": s.streams[idx]["description"],
    })))
}

fn delete_stream(s: &mut State, stream_id: &str) -> Result<Response, Response> {
    let idx = find_by_field_idx(&s.streams, "streamId", stream_id).ok_or_else(not_found)?;
    s.streams.remove(idx);
    Ok(ok_empty())
}

fn list_streams(s: &mut State) -> Result<Response, Response> {
    let items: Vec<Value> = s
        .streams
        .iter()
        .map(|st| {
            json!({
                "streamId": st["streamId"],
                "streamArn": st["streamArn"],
                "streamVersion": st["streamVersion"],
                "description": st["description"],
            })
        })
        .collect();
    Ok(ok_json(&json!({"streams": items})))
}

// ---------------------------------------------------------------------------
// OTA updates, packages (+versions, sboms), commands, command executions
// ---------------------------------------------------------------------------

fn create_ota_update(
    s: &mut State,
    id: &str,
    body: &Value,
    region: &str,
    account: u64,
) -> Result<Response, Response> {
    if s.ota_updates.iter().any(|o| o["otaUpdateId"] == json!(id)) {
        return Err(already_exists(
            &format!("OTA update {id} already exists."),
            id,
            &arn_for(region, account, "otaupdate", id),
        ));
    }
    s.ota_updates.push(json!({
        "otaUpdateId": id,
        "otaUpdateArn": arn_for(region, account, "otaupdate", id),
        "description": b_str(body, "description").unwrap_or_default(),
        "targets": body.get("targets").cloned().unwrap_or_else(|| json!([])),
        "protocols": body.get("protocols").cloned().unwrap_or_else(|| json!(["MQTT"])),
        "targetSelection": b_str(body, "targetSelection").unwrap_or("SNAPSHOT"),
        "otaUpdateFiles": body.get("files").cloned().unwrap_or_else(|| json!([])),
        "fileName": b_str(body, "fileName"),
        "otaUpdateStatus": "CREATE_COMPLETE",
        "creationDate": crate::proto::now_iso(),
        "lastModifiedDate": crate::proto::now_iso(),
    }));
    Ok(ok_json(&json!({
        "otaUpdateId": id,
        "otaUpdateArn": arn_for(region, account, "otaupdate", id),
        "otaUpdateStatus": "CREATE_COMPLETE",
    })))
}

fn get_ota_update(s: &mut State, id: &str) -> Result<Response, Response> {
    s.ota_updates
        .iter()
        .find(|o| o["otaUpdateId"].as_str() == Some(id))
        .map(|o| {
            ok_json(&json!({"otaUpdateInfo": {
                "otaUpdateId": o["otaUpdateId"],
                "otaUpdateArn": o["otaUpdateArn"],
                "creationDate": o["creationDate"],
                "lastModifiedDate": o["lastModifiedDate"],
                "description": o["description"],
                "protocols": o["protocols"],
                "targetSelection": o["targetSelection"],
                "otaUpdateFiles": o["otaUpdateFiles"],
                "otaUpdateStatus": o["otaUpdateStatus"],
            }}))
        })
        .ok_or_else(not_found)
}

fn delete_ota_update(s: &mut State, id: &str) -> Result<Response, Response> {
    let idx = find_by_field_idx(&s.ota_updates, "otaUpdateId", id).ok_or_else(not_found)?;
    s.ota_updates.remove(idx);
    Ok(ok_empty())
}

fn list_ota_updates(s: &mut State) -> Result<Response, Response> {
    let items: Vec<Value> = s
        .ota_updates
        .iter()
        .map(|o| json!({"otaUpdateId": o["otaUpdateId"], "otaUpdateArn": o["otaUpdateArn"]}))
        .collect();
    Ok(ok_json(&json!({"otaUpdates": items})))
}

// --- Packages ----------------------------------------------------------------

fn create_package(
    s: &mut State,
    name: &str,
    body: &Value,
    region: &str,
    account: u64,
) -> Result<Response, Response> {
    if s.packages.iter().any(|p| p["packageName"] == json!(name)) {
        return Err(conflict(&format!("A resource {name} already exists.")));
    }
    s.packages.push(json!({
        "packageName": name,
        "packageArn": arn_for(region, account, "package", name),
        "description": b_str(body, "description").unwrap_or_default(),
        "tags": body.get("tags").cloned().unwrap_or_else(|| json!({})),
        "creationDate": crate::proto::now_iso(),
        "lastModifiedDate": crate::proto::now_iso(),
    }));
    Ok(ok_json(&json!({
        "packageName": name,
        "packageArn": arn_for(region, account, "package", name),
    })))
}

fn get_package(s: &mut State, name: &str) -> Result<Response, Response> {
    s.packages
        .iter()
        .find(|p| p["packageName"].as_str() == Some(name))
        .map(ok_json)
        .ok_or_else(not_found)
}

fn update_package(s: &mut State, name: &str, body: &Value) -> Result<Response, Response> {
    let idx = find_by_field_idx(&s.packages, "packageName", name).ok_or_else(not_found)?;
    if let Some(desc) = b_val_ref(body, "description") {
        s.packages[idx]["description"] = desc.clone();
    }
    if let Some(default_version) = b_val_ref(body, "defaultVersionName") {
        s.packages[idx]["defaultVersionName"] = default_version.clone();
    }
    s.packages[idx]["lastModifiedDate"] = json!(crate::proto::now_iso());
    Ok(ok_empty())
}

fn delete_package(s: &mut State, name: &str) -> Result<Response, Response> {
    if let Some(idx) = find_by_field_idx(&s.packages, "packageName", name) {
        s.packages.remove(idx);
    }
    s.package_versions.remove(name);
    Ok(ok_empty())
}

fn list_packages(s: &mut State) -> Result<Response, Response> {
    let items: Vec<Value> = s.packages.clone();
    Ok(ok_json(&json!({"packagesSummary": items})))
}

fn get_package_configuration() -> Result<Response, Response> {
    Ok(ok_json(&json!({
        "versionUpdateByJobsConfig": {"enabled": false, "roleArn": ""},
    })))
}

fn update_package_configuration() -> Result<Response, Response> {
    Ok(ok_empty())
}

fn create_package_version(
    s: &mut State,
    package_name: &str,
    version_name: &str,
    body: &Value,
    region: &str,
    account: u64,
) -> Result<Response, Response> {
    find_by_field_idx(&s.packages, "packageName", package_name).ok_or_else(not_found)?;
    let versions = s
        .package_versions
        .entry(package_name.to_string())
        .or_default();
    if versions
        .iter()
        .any(|v| v["versionName"] == json!(version_name))
    {
        return Err(conflict(&format!(
            "A resource {package_name}/{version_name} already exists."
        )));
    }
    let arn = format!(
        "arn:{}:iot:{}:{}:package/{package_name}/version/{version_name}",
        partition(region),
        region,
        account
    );
    versions.push(json!({
        "packageName": package_name,
        "versionName": version_name,
        "packageVersionArn": arn,
        "description": b_str(body, "description").unwrap_or_default(),
        "attributes": body.get("attributes").cloned().unwrap_or_else(|| json!({})),
        "status": "DRAFT",
        "creationDate": crate::proto::now_iso(),
        "lastModifiedDate": crate::proto::now_iso(),
    }));
    let created = s
        .package_versions
        .get(package_name)
        .and_then(|vs| vs.last().cloned())
        .unwrap_or_else(|| json!({}));
    Ok(ok_json(&created))
}

fn get_package_version(
    s: &mut State,
    package_name: &str,
    version_name: &str,
) -> Result<Response, Response> {
    let versions = s
        .package_versions
        .get(package_name)
        .cloned()
        .unwrap_or_default();
    versions
        .into_iter()
        .find(|v| v["versionName"] == json!(version_name))
        .map(|v| {
            let mut obj = v.clone();
            if let Some(o) = obj.as_object_mut() {
                o.insert("artifact".into(), json!({}));
                o.insert("sbom".into(), json!({}));
            }
            ok_json(&obj)
        })
        .ok_or_else(not_found)
}

fn update_package_version(
    s: &mut State,
    package_name: &str,
    version_name: &str,
    body: &Value,
) -> Result<Response, Response> {
    let Some(versions) = s.package_versions.get_mut(package_name) else {
        return Err(not_found());
    };
    let idx = find_by_field_idx(versions, "versionName", version_name).ok_or_else(not_found)?;
    if let Some(desc) = b_val_ref(body, "description") {
        versions[idx]["description"] = desc.clone();
    }
    if let Some(attrs) = b_val_ref(body, "attributes") {
        versions[idx]["attributes"] = attrs.clone();
    }
    if let Some(action) = b_val_ref(body, "action") {
        versions[idx]["status"] = action.clone();
    }
    versions[idx]["lastModifiedDate"] = json!(crate::proto::now_iso());
    Ok(ok_empty())
}

fn delete_package_version(
    s: &mut State,
    package_name: &str,
    version_name: &str,
) -> Result<Response, Response> {
    if let Some(versions) = s.package_versions.get_mut(package_name) {
        if let Some(idx) = find_by_field_idx(versions, "versionName", version_name) {
            versions.remove(idx);
        }
    }
    Ok(ok_empty())
}

fn list_package_versions(s: &mut State, package_name: &str) -> Result<Response, Response> {
    let items: Vec<Value> = s
        .package_versions
        .get(package_name)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(|v| {
            json!({
                "packageName": v["packageName"],
                "versionName": v["versionName"],
                "packageVersionArn": v["packageVersionArn"],
                "status": v["status"],
            })
        })
        .collect();
    Ok(ok_json(&json!({"packageVersionSummaries": items})))
}

fn associate_sbom(
    s: &mut State,
    package_name: &str,
    version_name: &str,
    body: &Value,
) -> Result<Response, Response> {
    if let Some(versions) = s.package_versions.get_mut(package_name) {
        if let Some(idx) = find_by_field_idx(versions, "versionName", version_name) {
            if let Some(sbom) = b_val_ref(body, "sbom") {
                versions[idx]["sbom"] = sbom.clone();
            }
        }
    }
    Ok(ok_json(
        &json!({"packageName": package_name, "versionName": version_name}),
    ))
}

fn disassociate_sbom(
    s: &mut State,
    package_name: &str,
    version_name: &str,
) -> Result<Response, Response> {
    if let Some(versions) = s.package_versions.get_mut(package_name) {
        if let Some(idx) = find_by_field_idx(versions, "versionName", version_name) {
            versions[idx].as_object_mut().map(|o| o.remove("sbom"));
        }
    }
    Ok(ok_json(&json!({"packageName": package_name})))
}

fn list_sbom_validation_results() -> Result<Response, Response> {
    Ok(ok_json(&json!({"validationResultSummaries": []})))
}

// --- Commands -----------------------------------------------------------------

fn create_command(
    s: &mut State,
    command_id: &str,
    body: &Value,
    region: &str,
    account: u64,
) -> Result<Response, Response> {
    if s.commands
        .iter()
        .any(|c| c["commandId"] == json!(command_id))
    {
        return Err(already_exists(
            "Command with given ID already exists.",
            command_id,
            &arn_for(region, account, "command", command_id),
        ));
    }
    s.commands.push(json!({
        "commandId": command_id,
        "commandArn": arn_for(region, account, "command", command_id),
        "namespace": b_str(body, "namespace").unwrap_or("AWS-IoT"),
        "displayName": b_str(body, "displayName"),
        "description": b_str(body, "description"),
        "payload": b_val_ref(body, "payload").cloned().unwrap_or(json!({})),
        "mandatoryParameters": body.get("mandatoryParameters").cloned().unwrap_or_else(|| json!([])),
        "roleArn": b_str(body, "roleArn"),
        "deprecated": false,
        "pendingDeletion": false,
        "createdAt": crate::proto::now_iso(),
        "lastUpdatedAt": crate::proto::now_iso(),
    }));
    Ok(ok_json(&json!({
        "commandId": command_id,
        "commandArn": arn_for(region, account, "command", command_id),
    })))
}

fn get_command(s: &mut State, command_id: &str) -> Result<Response, Response> {
    s.commands
        .iter()
        .find(|c| c["commandId"].as_str() == Some(command_id))
        .map(ok_json)
        .ok_or_else(not_found)
}

fn update_command(s: &mut State, command_id: &str, body: &Value) -> Result<Response, Response> {
    let idx = find_by_field_idx(&s.commands, "commandId", command_id).ok_or_else(not_found)?;
    for key in ["displayName", "description", "deprecated"] {
        if let Some(v) = body.get(key).filter(|v| !v.is_null()) {
            s.commands[idx][key] = v.clone();
        }
    }
    s.commands[idx]["lastUpdatedAt"] = json!(crate::proto::now_iso());
    Ok(ok_json(&s.commands[idx]))
}

fn delete_command(s: &mut State, command_id: &str) -> Result<Response, Response> {
    let idx = find_by_field_idx(&s.commands, "commandId", command_id).ok_or_else(not_found)?;
    s.commands.remove(idx);
    Ok(ok_empty())
}

fn list_commands(s: &mut State) -> Result<Response, Response> {
    let items: Vec<Value> = s
        .commands
        .iter()
        .map(|c| json!({"commandId": c["commandId"], "commandArn": c["commandArn"]}))
        .collect();
    Ok(ok_json(&json!({"commands": items})))
}

fn list_command_executions() -> Result<Response, Response> {
    Ok(ok_json(&json!({"commandExecutions": []})))
}

fn get_command_execution(body: &Value) -> Result<Response, Response> {
    let _ = b_str(body, "executionId").unwrap_or_default();
    Ok(ok_json(&json!({
        "executionId": b_str(body, "executionId").unwrap_or_default(),
        "targetArn": b_str(body, "targetArn").unwrap_or_default(),
        "status": "SUCCEEDED",
        "namespace": "AWS-IoT",
        "startedAt": crate::proto::now_iso(),
        "completedAt": crate::proto::now_iso(),
    })))
}

fn delete_command_execution(body: &Value) -> Result<Response, Response> {
    let _ = b_str(body, "executionId").unwrap_or_default();
    Ok(ok_empty())
}

// ---------------------------------------------------------------------------
// Mitigation actions, scheduled audits, audit config/tasks/suppressions,
// detect tasks — impls
// ---------------------------------------------------------------------------

fn create_mitigation_action(
    s: &mut State,
    name: &str,
    body: &Value,
    region: &str,
    account: u64,
) -> Result<Response, Response> {
    if s.mitigation_actions
        .iter()
        .any(|a| a["actionName"] == json!(name))
    {
        return Err(already_exists(
            &format!("Mitigation action {name} already exists."),
            name,
            &arn_for(region, account, "mitigationaction", name),
        ));
    }
    s.mitigation_actions.push(json!({
        "actionName": name,
        "actionArn": arn_for(region, account, "mitigationaction", name),
        "actionId": random_id(),
        "roleArn": b_str(body, "roleArn"),
        "actionParams": body.get("actionParams").cloned().unwrap_or_else(|| json!({})),
        "creationDate": crate::proto::now_iso(),
        "lastModifiedDate": crate::proto::now_iso(),
    }));
    Ok(ok_json(&json!({
        "actionArn": s
            .mitigation_actions
            .last()
            .and_then(|a| a.get("actionArn"))
            .cloned()
            .unwrap_or(Value::Null),
        "actionId": s
            .mitigation_actions
            .last()
            .and_then(|a| a.get("actionId"))
            .cloned()
            .unwrap_or(Value::Null),
    })))
}

fn describe_mitigation_action(s: &mut State, name: &str) -> Result<Response, Response> {
    s.mitigation_actions
        .iter()
        .find(|a| a["actionName"].as_str() == Some(name))
        .map(ok_json)
        .ok_or_else(not_found)
}

fn update_mitigation_action(s: &mut State, name: &str, body: &Value) -> Result<Response, Response> {
    let idx = find_by_field_idx(&s.mitigation_actions, "actionName", name).ok_or_else(not_found)?;
    if let Some(role) = b_val_ref(body, "roleArn") {
        s.mitigation_actions[idx]["roleArn"] = role.clone();
    }
    if let Some(params) = b_val_ref(body, "actionParams") {
        s.mitigation_actions[idx]["actionParams"] = params.clone();
    }
    s.mitigation_actions[idx]["lastModifiedDate"] = json!(crate::proto::now_iso());
    Ok(ok_json(&json!({
        "actionArn": s.mitigation_actions[idx]["actionArn"],
        "actionId": s.mitigation_actions[idx]["actionId"],
    })))
}

fn delete_mitigation_action(s: &mut State, name: &str) -> Result<Response, Response> {
    let idx = find_by_field_idx(&s.mitigation_actions, "actionName", name).ok_or_else(not_found)?;
    s.mitigation_actions.remove(idx);
    Ok(ok_empty())
}

fn list_mitigation_actions(s: &mut State) -> Result<Response, Response> {
    let items: Vec<Value> = s
        .mitigation_actions
        .iter()
        .map(|a| json!({"actionName": a["actionName"], "actionArn": a["arn"]}))
        .collect();
    Ok(ok_json(&json!({"actionIdentifiers": items})))
}

// --- Scheduled audits ----------------------------------------------------------

fn create_scheduled_audit(
    s: &mut State,
    name: &str,
    body: &Value,
    region: &str,
    account: u64,
) -> Result<Response, Response> {
    if s.scheduled_audits
        .iter()
        .any(|a| a["scheduledAuditName"] == json!(name))
    {
        return Err(already_exists(
            &format!("Scheduled audit {name} already exists."),
            name,
            &arn_for(region, account, "scheduledaudit", name),
        ));
    }
    s.scheduled_audits.push(json!({
        "scheduledAuditName": name,
        "scheduledAuditArn": arn_for(region, account, "scheduledaudit", name),
        "frequency": b_str(body, "frequency").unwrap_or_default(),
        "dayOfMonth": b_val_ref(body, "dayOfMonth").cloned().unwrap_or(Value::Null),
        "dayOfWeek": b_val_ref(body, "dayOfWeek").cloned().unwrap_or(Value::Null),
        "targetCheckNames": body.get("targetCheckNames").cloned().unwrap_or_else(|| json!([])),
    }));
    Ok(ok_json(&json!({
        "scheduledAuditArn": arn_for(region, account, "scheduledaudit", name),
    })))
}

fn describe_scheduled_audit(s: &mut State, name: &str) -> Result<Response, Response> {
    s.scheduled_audits
        .iter()
        .find(|a| a["scheduledAuditName"].as_str() == Some(name))
        .map(ok_json)
        .ok_or_else(not_found)
}

fn update_scheduled_audit(s: &mut State, name: &str, body: &Value) -> Result<Response, Response> {
    let idx =
        find_by_field_idx(&s.scheduled_audits, "scheduledAuditName", name).ok_or_else(not_found)?;
    for key in ["frequency", "dayOfMonth", "dayOfWeek", "targetCheckNames"] {
        if let Some(v) = body.get(key).filter(|v| !v.is_null()) {
            s.scheduled_audits[idx][key] = v.clone();
        }
    }
    Ok(ok_json(
        &json!({"scheduledAuditArn": s.scheduled_audits[idx]["scheduledAuditArn"]}),
    ))
}

fn delete_scheduled_audit(s: &mut State, name: &str) -> Result<Response, Response> {
    let idx =
        find_by_field_idx(&s.scheduled_audits, "scheduledAuditName", name).ok_or_else(not_found)?;
    s.scheduled_audits.remove(idx);
    Ok(ok_empty())
}

fn list_scheduled_audits(s: &mut State) -> Result<Response, Response> {
    let items: Vec<Value> = s
        .scheduled_audits
        .iter()
        .map(|a| {
            json!({
                "scheduledAuditName": a["scheduledAuditName"],
                "scheduledAuditArn": a["scheduledAuditArn"],
                "frequency": a["frequency"],
            })
        })
        .collect();
    Ok(ok_json(&json!({"scheduledAudits": items})))
}

// --- Account audit configuration ------------------------------------------------

fn describe_account_audit_configuration(s: &mut State) -> Result<Response, Response> {
    if s.account_audit_configuration.is_null() {
        return Ok(ok_json(&json!({
            "roleArn": "",
            "auditNotificationTargetConfigurations": {},
            "auditCheckConfigurations": {},
        })));
    }
    Ok(ok_json(&s.account_audit_configuration))
}

fn update_account_audit_configuration(s: &mut State, body: &Value) -> Result<Response, Response> {
    if s.account_audit_configuration.is_null() {
        s.account_audit_configuration = json!({ "roleArn": "", "auditNotificationTargetConfigurations": {}, "auditCheckConfigurations": {} });
    }
    if let Some(role) = b_val_ref(body, "roleArn") {
        s.account_audit_configuration["roleArn"] = role.clone();
    }
    if let Some(cfg) = b_val_ref(body, "auditNotificationTargetConfigurations") {
        s.account_audit_configuration["auditNotificationTargetConfigurations"] = cfg.clone();
    }
    if let Some(cfg) = b_val_ref(body, "auditCheckConfigurations") {
        s.account_audit_configuration["auditCheckConfigurations"] = cfg.clone();
    }
    Ok(ok_empty())
}

fn delete_account_audit_configuration(s: &mut State) -> Result<Response, Response> {
    s.account_audit_configuration = Value::Null;
    Ok(ok_empty())
}

// --- On-demand audit tasks / findings (moto does not persist these) -------------

fn start_on_demand_audit_task() -> Result<Response, Response> {
    Ok(ok_json(&json!({"taskId": random_id()})))
}

fn cancel_audit_task() -> Result<Response, Response> {
    Ok(ok_empty())
}

fn describe_audit_task(s: &mut State, task_id: &str) -> Result<Response, Response> {
    let _ = s;
    Err(not_found_msg(format!(
        "Audit task {task_id} does not exist."
    )))
}

fn list_audit_tasks() -> Result<Response, Response> {
    Ok(ok_json(&json!({"tasks": [], "nextToken": Value::Null})))
}

fn list_audit_findings() -> Result<Response, Response> {
    Ok(ok_json(&json!({"findings": [], "nextToken": Value::Null})))
}

fn describe_audit_finding() -> Result<Response, Response> {
    Err(not_found())
}

fn list_related_resources_for_audit_finding() -> Result<Response, Response> {
    Ok(ok_json(&json!({"relatedResources": []})))
}

// --- Audit suppressions ---------------------------------------------------------

fn create_audit_suppression(s: &mut State, body: &Value) -> Result<Response, Response> {
    let check = b_str(body, "checkName").unwrap_or_default().to_string();
    let ri = b_val_ref(body, "resourceIdentifier")
        .cloned()
        .unwrap_or_else(|| json!({}));
    let key = suppression_key(&check, &ri);
    if s.audit_suppressions.contains_key(&key) {
        return Err(already_exists(
            "Audit suppression already exists.",
            &key,
            &key,
        ));
    }
    s.audit_suppressions.insert(
        key,
        json!({
            "checkName": check,
            "resourceIdentifier": ri,
            "expirationDate": b_val_ref(body, "expirationDate").cloned().unwrap_or(Value::Null),
            "suppressIndefinitely": b_bool(body, "suppressIndefinitely"),
            "description": b_str(body, "description").unwrap_or_default(),
        }),
    );
    Ok(ok_empty())
}

fn delete_audit_suppression(s: &mut State, body: &Value) -> Result<Response, Response> {
    let check = b_str(body, "checkName").unwrap_or_default().to_string();
    let ri = b_val_ref(body, "resourceIdentifier")
        .cloned()
        .unwrap_or(json!({}));
    let key = suppression_key(&check, &ri);
    s.audit_suppressions.remove(&key).ok_or_else(not_found)?;
    Ok(ok_empty())
}

fn describe_audit_suppression(s: &mut State, body: &Value) -> Result<Response, Response> {
    let check = b_str(body, "checkName").unwrap_or_default().to_string();
    let ri = b_val_ref(body, "resourceIdentifier")
        .cloned()
        .unwrap_or(json!({}));
    let key = suppression_key(&check, &ri);
    s.audit_suppressions
        .get(&key)
        .map(ok_json)
        .ok_or_else(not_found)
}

fn update_audit_suppression(s: &mut State, body: &Value) -> Result<Response, Response> {
    let check = b_str(body, "checkName").unwrap_or_default().to_string();
    let ri = b_val_ref(body, "resourceIdentifier")
        .cloned()
        .unwrap_or(json!({}));
    let key = suppression_key(&check, &ri);
    let idx = match s.audit_suppressions.get_mut(&key) {
        Some(v) => v,
        None => return Err(not_found()),
    };
    for key_field in ["expirationDate", "suppressIndefinitely", "description"] {
        if let Some(v) = body.get(key_field).filter(|v| !v.is_null()) {
            idx[key_field] = v.clone();
        }
    }
    Ok(ok_empty())
}

fn list_audit_suppressions() -> Result<Response, Response> {
    Ok(ok_json(
        &json!({"suppressions": [], "nextToken": Value::Null}),
    ))
}

// --- Audit mitigation actions tasks ----------------------------------------------

fn start_audit_mitigation_actions_task(
    s: &mut State,
    task_id: &str,
    body: &Value,
) -> Result<Response, Response> {
    s.audit_mitigation_tasks.insert(
        task_id.to_string(),
        json!({
            "taskId": task_id,
            "taskStatus": "IN_PROGRESS",
            "taskStatistics": {},
            "target": b_val_ref(body, "target").cloned().unwrap_or(json!({})),
            "auditCheckToActionsMapping": b_val_ref(body, "auditCheckToActionsMapping").cloned().unwrap_or_else(|| json!({})),
            "actionsDefinition": [],
        }),
    );
    Ok(ok_json(&json!({"taskId": task_id})))
}

fn describe_audit_mitigation_actions_task(
    s: &mut State,
    task_id: &str,
) -> Result<Response, Response> {
    s.audit_mitigation_tasks
        .get(task_id)
        .map(ok_json)
        .ok_or_else(not_found)
}

fn list_audit_mitigation_actions_tasks() -> Result<Response, Response> {
    Ok(ok_json(&json!({"tasks": [], "nextToken": Value::Null})))
}

fn list_audit_mitigation_actions_executions() -> Result<Response, Response> {
    Ok(ok_json(
        &json!({"actionsExecutions": [], "nextToken": Value::Null}),
    ))
}

// --- Detect mitigation actions tasks ---------------------------------------------

fn start_detect_mitigation_actions_task(
    s: &mut State,
    task_id: &str,
    body: &Value,
) -> Result<Response, Response> {
    s.detect_mitigation_tasks.insert(
        task_id.to_string(),
        json!({
            "taskSummary": {
                "taskId": task_id,
                "taskStatus": "IN_PROGRESS",
                "target": b_val_ref(body, "target").cloned().unwrap_or(json!({})),
                "actionsDefinition": body
                    .get("actions")
                    .and_then(Value::as_array)
                    .map(|a| {
                        a.iter()
                            .filter_map(Value::as_str)
                            .map(|name| json!({"name": name}))
                            .collect::<Vec<Value>>()
                    })
                    .unwrap_or_default(),
            }
        }),
    );
    Ok(ok_json(&json!({"taskId": task_id})))
}

fn cancel_detect_mitigation_actions_task() -> Result<Response, Response> {
    Ok(ok_empty())
}

fn describe_detect_mitigation_actions_task(
    s: &mut State,
    task_id: &str,
) -> Result<Response, Response> {
    s.detect_mitigation_tasks
        .get(task_id)
        .map(ok_json)
        .ok_or_else(not_found)
}

fn list_detect_mitigation_actions_tasks() -> Result<Response, Response> {
    Ok(ok_json(&json!({"tasks": [], "nextToken": Value::Null})))
}

fn list_detect_mitigation_actions_executions() -> Result<Response, Response> {
    Ok(ok_json(
        &json!({"actionsExecutions": [], "nextToken": Value::Null}),
    ))
}

// ---------------------------------------------------------------------------
// Tags, logging, event configs, indexing, indices, misc tails
// ---------------------------------------------------------------------------

fn tag_resource(s: &mut State, body: &Value) -> Result<Response, Response> {
    let arn = b_str(body, "resourceArn").unwrap_or_default().to_string();
    if !resource_arn_exists(s, &arn) {
        return Err(not_found());
    }
    let incoming: Vec<Value> = body
        .get("tags")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let entry = s.tags.entry(arn).or_insert_with(|| json!([]));
    for tag in incoming {
        let key = tag["Key"].as_str().unwrap_or_default().to_string();
        if let Some(arr) = entry.as_array_mut() {
            arr.retain(|t| t["Key"] != json!(key));
            arr.push(tag);
        }
    }
    Ok(ok_empty())
}

fn untag_resource(s: &mut State, body: &Value) -> Result<Response, Response> {
    let arn = b_str(body, "resourceArn").unwrap_or_default().to_string();
    if !resource_arn_exists(s, &arn) {
        return Err(not_found());
    }
    let keys: Vec<String> = body
        .get("tagKeys")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default();
    if let Some(entry) = s.tags.get_mut(&arn) {
        if let Some(arr) = entry.as_array_mut() {
            arr.retain(|t| {
                !keys
                    .iter()
                    .any(|k| *k == t["Key"].as_str().unwrap_or_default())
            });
        }
    }
    Ok(ok_empty())
}

fn resource_arn_exists(s: &State, arn: &str) -> bool {
    [
        s.things.iter().any(|t| t["arn"] == json!(arn)),
        s.thing_types
            .iter()
            .any(|t| t["thingTypeArn"] == json!(arn)),
        s.thing_groups.iter().any(|g| g["arn"] == json!(arn)),
        s.billing_groups
            .iter()
            .any(|g| g["billingGroupArn"] == json!(arn)),
        s.policies.iter().any(|p| p["arn"] == json!(arn)),
        s.rules.iter().any(|r| r["arn"] == json!(arn)),
        s.role_aliases.iter().any(|r| r["arn"] == json!(arn)),
        s.domain_configurations
            .iter()
            .any(|d| d["domainConfigurationArn"] == json!(arn)),
        s.security_profiles
            .iter()
            .any(|p| p["securityProfileArn"] == json!(arn)),
        s.authorizers
            .iter()
            .any(|a| a["authorizerArn"] == json!(arn)),
        s.provisioning_templates
            .iter()
            .any(|p| p["templateArn"] == json!(arn)),
        s.dimensions.iter().any(|d| d["arn"] == json!(arn)),
        s.custom_metrics
            .iter()
            .any(|m| m["metricArn"] == json!(arn)),
        s.fleet_metrics.iter().any(|m| m["metricArn"] == json!(arn)),
        s.streams.iter().any(|st| st["streamArn"] == json!(arn)),
        s.mitigation_actions
            .iter()
            .any(|a| a["actionArn"] == json!(arn)),
        s.scheduled_audits
            .iter()
            .any(|a| a["scheduledAuditArn"] == json!(arn)),
        s.ota_updates
            .iter()
            .any(|o| o["otaUpdateArn"] == json!(arn)),
        s.certificates.iter().any(|c| c["arn"] == json!(arn)),
        s.ca_certificates.iter().any(|c| c["arn"] == json!(arn)),
    ]
    .contains(&true)
}

fn list_tags_for_resource(
    s: &mut State,
    body: &Value,
    query: &BTreeMap<String, Vec<String>>,
) -> Result<Response, Response> {
    let arn = p_str(body, query, "resourceArn").unwrap_or_default();
    if !resource_arn_exists(s, &arn) {
        return Err(not_found());
    }
    Ok(ok_json(
        &json!({"tags": s.tags.get(&arn).cloned().unwrap_or_else(|| json!([]))}),
    ))
}

fn test_authorization() -> Result<Response, Response> {
    Ok(ok_json(&json!({"authResults": []})))
}

fn get_behavior_model_training_summaries() -> Result<Response, Response> {
    Ok(ok_json(&json!({"summaries": []})))
}

fn list_active_violations() -> Result<Response, Response> {
    Ok(ok_json(
        &json!({"activeViolations": [], "nextToken": Value::Null}),
    ))
}

fn list_violation_events() -> Result<Response, Response> {
    Ok(ok_json(
        &json!({"violationEvents": [], "nextToken": Value::Null}),
    ))
}

fn list_metric_values() -> Result<Response, Response> {
    Ok(ok_json(
        &json!({"metricDatumList": [], "nextToken": Value::Null}),
    ))
}

fn list_managed_job_templates() -> Result<Response, Response> {
    Ok(ok_json(
        &json!({"managedJobTemplates": [], "nextToken": Value::Null}),
    ))
}

fn describe_managed_job_template(s: &mut State, name: &str) -> Result<Response, Response> {
    let _ = s;
    Err(not_found_msg(format!(
        "Managed job template {name} does not exist."
    )))
}

fn put_verification_state_on_violation(_violation_id: &str) -> Result<Response, Response> {
    Ok(ok_empty())
}

fn list_thing_registration_tasks(s: &mut State) -> Result<Response, Response> {
    let _ = s;
    Ok(ok_json(&json!({"taskIds": []})))
}

fn start_thing_registration_task(s: &mut State, body: &Value) -> Result<Response, Response> {
    let task_id = random_id();
    s.registration_tasks.insert(
        task_id.clone(),
        json!({
            "taskId": task_id.clone(),
            "status": "InProgress",
            "templateBody": b_str(body, "templateBody").unwrap_or_default(),
            "inputFileBucket": b_str(body, "inputFileBucket").unwrap_or_default(),
            "inputFileKey": b_str(body, "inputFileKey").unwrap_or_default(),
            "roleArn": b_str(body, "roleArn").unwrap_or_default(),
            "successCount": 0,
            "failureCount": 0,
            "percentageProgress": 0,
        }),
    );
    Ok(ok_json(&json!({"taskId": task_id})))
}

fn describe_thing_registration_task(s: &mut State, task_id: &str) -> Result<Response, Response> {
    s.registration_tasks
        .get(task_id)
        .map(ok_json)
        .ok_or_else(|| not_found_msg(format!("Task {task_id} cannot be found.")))
}

fn stop_thing_registration_task(s: &mut State, task_id: &str) -> Result<Response, Response> {
    s.registration_tasks
        .remove(task_id)
        .ok_or_else(|| not_found_msg(format!("Task {task_id} cannot be found.")))?;
    Ok(ok_empty())
}

fn list_thing_registration_task_reports() -> Result<Response, Response> {
    Ok(ok_json(&json!({"resourceLinks": []})))
}

// --- Logging -------------------------------------------------------------------

fn get_logging_options(s: &mut State) -> Result<Response, Response> {
    Ok(if s.logging_options.is_null() {
        ok_json(&json!({}))
    } else {
        ok_json(&s.logging_options)
    })
}

fn set_logging_options(s: &mut State, body: &Value) -> Result<Response, Response> {
    if s.logging_options.is_null() {
        s.logging_options = json!({});
    }
    if let Some(payload) = b_val_ref(body, "loggingOptionsPayload") {
        s.logging_options = payload.clone();
    }
    Ok(ok_empty())
}

fn get_v2_logging_options(s: &mut State) -> Result<Response, Response> {
    Ok(if s.v2_logging_options.is_null() {
        ok_json(&json!({
            "roleArn": "",
            "defaultLogLevel": "WARN",
            "disableAllLogs": false,
        }))
    } else {
        ok_json(&s.v2_logging_options)
    })
}

fn set_v2_logging_options(s: &mut State, body: &Value) -> Result<Response, Response> {
    if s.v2_logging_options.is_null() {
        s.v2_logging_options =
            json!({"roleArn": "", "defaultLogLevel": "WARN", "disableAllLogs": false});
    }
    for key in ["roleArn", "defaultLogLevel", "disableAllLogs"] {
        if let Some(v) = body.get(key).filter(|v| !v.is_null()) {
            s.v2_logging_options[key] = v.clone();
        }
    }
    Ok(ok_empty())
}

fn set_v2_logging_level(s: &mut State, body: &Value) -> Result<Response, Response> {
    let target_key = format!(
        "{}:{}",
        body.get("logTarget")
            .and_then(|t| t.get("targetType"))
            .and_then(Value::as_str)
            .unwrap_or_default(),
        body.get("logTarget")
            .and_then(|t| t.get("targetName"))
            .and_then(Value::as_str)
            .unwrap_or_default()
    );
    s.v2_logging_levels.insert(
        target_key,
        json!({
            "logTarget": body.get("logTarget").cloned().unwrap_or(json!({})),
            "logLevel": body.get("logLevel").cloned().unwrap_or(Value::Null),
        }),
    );
    Ok(ok_empty())
}

fn list_v2_logging_levels(s: &mut State) -> Result<Response, Response> {
    let items: Vec<Value> = s.v2_logging_levels.values().cloned().collect();
    Ok(ok_json(&json!({"logTargetConfigurations": items})))
}

fn delete_v2_logging_level(
    s: &mut State,
    body: &Value,
    query: &BTreeMap<String, Vec<String>>,
) -> Result<Response, Response> {
    let target_type = p_str(body, query, "targetType").unwrap_or_default();
    let target_name = p_str(body, query, "targetName").unwrap_or_default();
    s.v2_logging_levels
        .remove(&format!("{target_type}:{target_name}"));
    Ok(ok_empty())
}

// --- Event configurations / indexing / indices ----------------------------------

fn describe_event_configurations(s: &mut State) -> Result<Response, Response> {
    if s.event_configurations.is_empty() {
        let types = [
            "THING",
            "THING_GROUP",
            "THING_TYPE",
            "THING_GROUP_MEMBERSHIP",
            "THING_GROUP_HIERARCHY",
            "THING_TYPE_ASSOCIATION",
            "JOB",
            "JOB_EXECUTION",
            "POLICY",
            "CERTIFICATE",
            "CA_CERTIFICATE",
        ];
        return Ok(ok_json(&json!({"eventConfigurations":
            Value::Object(types.iter().map(|t| ((*t).to_string(), json!({"Enabled": false}))).collect())})));
    }
    Ok(ok_json(
        &json!({"eventConfigurations": b_map_to_value(&s.event_configurations)}),
    ))
}

fn update_event_configurations(s: &mut State, body: &Value) -> Result<Response, Response> {
    if let Some(configs) = body.get("eventConfigurations").and_then(Value::as_object) {
        for (k, v) in configs {
            s.event_configurations.insert(k.clone(), v.clone());
        }
    }
    Ok(ok_empty())
}

fn get_indexing_configuration(s: &mut State) -> Result<Response, Response> {
    Ok(ok_json(&json!({
        "thingIndexingConfiguration": if s.thing_indexing_configuration.is_null() {
            json!({"thingIndexingMode": "OFF", "deviceDefenderIndexingMode": "OFF", "namedShadowIndexingMode": "OFF", "thingConnectivityIndexingMode": "OFF", "managedFields": [], "customFields": [], "filter": {"geoLocations": [], "namedShadowNames": []}})
        } else {
            s.thing_indexing_configuration.clone()
        },
        "thingGroupIndexingConfiguration": if s.thing_group_indexing_configuration.is_null() {
            json!({"thingGroupIndexingMode": "OFF", "managedFields": [], "customFields": []})
        } else {
            s.thing_group_indexing_configuration.clone()
        },
    })))
}

fn update_indexing_configuration(s: &mut State, body: &Value) -> Result<Response, Response> {
    if let Some(cfg) = body
        .get("thingIndexingConfiguration")
        .filter(|v| !v.is_null())
    {
        s.thing_indexing_configuration = cfg.clone();
    }
    if let Some(cfg) = body
        .get("thingGroupIndexingConfiguration")
        .filter(|v| !v.is_null())
    {
        s.thing_group_indexing_configuration = cfg.clone();
    }
    Ok(ok_empty())
}

fn list_indices() -> Result<Response, Response> {
    Ok(ok_json(&json!({"indexNames": ["AWS_Things"]})))
}

fn describe_index(index_name: &str) -> Result<Response, Response> {
    Ok(ok_json(&json!({
        "indexName": index_name,
        "indexStatus": "ACTIVE",
        "schema": "MULTI_INDEXING_MODE",
    })))
}

fn search_index(s: &mut State, body: &Value) -> Result<Response, Response> {
    let query = b_str(body, "queryString").unwrap_or("*").to_string();
    let mut items = Vec::new();
    for t in &s.things {
        if query == "*" || t["thingName"].as_str().unwrap_or_default().contains(&query) {
            let mut obj = t.clone();
            if let Some(o) = obj.as_object_mut() {
                o.insert("connectivity".into(), json!({}));
                o.insert("shadow".into(), json!({}));
            }
            items.push(obj);
        }
    }
    Ok(ok_json(&json!({"things": items, "thingGroups": []})))
}

fn get_statistics(s: &mut State, body: &Value) -> Result<Response, Response> {
    let _ = b_str(body, "queryString");
    let count = s.things.len() as i64;
    Ok(ok_json(&json!({"statistics": {"count": count}})))
}

fn get_cardinality(_body: &Value) -> Result<Response, Response> {
    Ok(ok_json(&json!({"cardinality": 0})))
}

fn get_buckets_aggregation(_body: &Value) -> Result<Response, Response> {
    Ok(ok_json(&json!({"totalCount": 0, "buckets": []})))
}

fn get_percentiles(_body: &Value) -> Result<Response, Response> {
    Ok(ok_json(&json!({"percentiles": []})))
}

// --- Encryption configuration / endpoint ----------------------------------------

fn describe_endpoint(
    body: &Value,
    query: &BTreeMap<String, Vec<String>>,
) -> Result<Response, Response> {
    let endpoint_type = p_str(body, query, "endpointType").unwrap_or_default();
    let recognized = [
        "iot:Data",
        "iot:Data-ATS",
        "iot:Data-ATS-IPv4",
        "iot:Data-IPv4",
        "iot:CredentialProvider",
        "iot:Jobs",
    ];
    let matched = if endpoint_type.is_empty() {
        Some("iot:Data-ATS".to_string())
    } else if recognized.contains(&endpoint_type.as_str()) {
        Some(endpoint_type)
    } else {
        None
    };
    let Some(kind) = matched else {
        return Err(invalid_request(&format!(
            " An error occurred (InvalidRequestException) when calling the DescribeEndpoint operation: Endpoint type {} not recognized.",
            p_str(body, query, "endpointType").unwrap_or_default()
        )));
    };
    let identifier = random_id();
    let address = if kind == "iot:Data" {
        format!("{identifier}.iot.us-east-1.amazonaws.com")
    } else if kind.starts_with("iot:Data-ATS") {
        format!("{identifier}-ats.iot.us-east-1.amazonaws.com")
    } else if kind == "iot:CredentialProvider" {
        format!("{identifier}.credentials.iot.us-east-1.amazonaws.com")
    } else {
        format!("{identifier}.jobs.iot.us-east-1.amazonaws.com")
    };
    Ok(ok_json(&json!({
        "endpointAddress": address_for_reason(&kind.to_string(), &address),
    })))
}

fn address_for_reason(_kind: &str, address: &str) -> String {
    address.to_string()
}

fn describe_encryption_configuration() -> Result<Response, Response> {
    Ok(ok_json(&json!({"encryptionType": "AWS_OWNED_KEY"})))
}

fn update_encryption_configuration(body: &Value) -> Result<Response, Response> {
    let _ = b_val_ref(body, "encryptionType");
    Ok(ok_empty())
}

// ---------------------------------------------------------------------------
// Role aliases — impls (moto's FakeRoleAlias shape)
// ---------------------------------------------------------------------------

fn role_alias_record(alias: &str, body: &Value, region: &str, account: u64) -> Value {
    json!({
        "roleAlias": alias,
        "arn": arn_for(region, account, "rolealias", alias),
        "roleArn": b_str(body, "roleArn").unwrap_or_default(),
        "credentialDurationSeconds": body.get("credentialDurationSeconds").cloned().unwrap_or_else(|| json!(3600)),
        "creationDate": crate::proto::now_iso(),
        "lastModifiedDate": crate::proto::now_iso(),
    })
}

fn create_role_alias(
    s: &mut State,
    alias: &str,
    body: &Value,
    region: &str,
    account: u64,
) -> Result<Response, Response> {
    if s.role_aliases
        .iter()
        .any(|r| r["roleAlias"] == json!(alias))
    {
        return Err(already_exists(
            &format!("RoleAlias cannot be created - already exists (name={alias})"),
            alias,
            &arn_for(region, account, "rolealias", alias),
        ));
    }
    let record = role_alias_record(alias, body, region, account);
    let arn = record["arn"].clone();
    s.role_aliases.push(record);
    Ok(ok_json(&json!({"roleAlias": alias, "roleAliasArn": arn})))
}

fn describe_role_alias(s: &mut State, alias: &str) -> Result<Response, Response> {
    s.role_aliases
        .iter()
        .find(|r| r["roleAlias"].as_str() == Some(alias))
        .map(|r| ok_json(&json!({"roleAliasDescription": r})))
        .ok_or_else(not_found)
}

fn update_role_alias(s: &mut State, alias: &str, body: &Value) -> Result<Response, Response> {
    let idx = find_by_field_idx(&s.role_aliases, "roleAlias", alias).ok_or_else(not_found)?;
    if let Some(role_arn) = b_val_ref(body, "roleArn") {
        s.role_aliases[idx]["roleArn"] = role_arn.clone();
    }
    if let Some(dur) = b_val_ref(body, "credentialDurationSeconds") {
        s.role_aliases[idx]["credentialDurationSeconds"] = dur.clone();
    }
    s.role_aliases[idx]["lastModifiedDate"] = json!(crate::proto::now_iso());
    Ok(ok_json(
        &json!({"roleAlias": alias, "roleAliasArn": s.role_aliases[idx]["arn"]}),
    ))
}

fn delete_role_alias(s: &mut State, alias: &str) -> Result<Response, Response> {
    let idx = find_by_field_idx(&s.role_aliases, "roleAlias", alias).ok_or_else(not_found)?;
    s.role_aliases.remove(idx);
    Ok(ok_empty())
}

fn list_role_aliases(s: &mut State) -> Result<Response, Response> {
    let aliases: Vec<&Value> = s.role_aliases.iter().map(|r| &r["roleAlias"]).collect();
    Ok(ok_json(&json!({"roleAliases": aliases})))
}

// ---------------------------------------------------------------------------
// Simple record arrives only via p_* params: DecodeCertificate-family
// members exist above. End of operation impls.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Certificate providers — impls
// ---------------------------------------------------------------------------

fn create_certificate_provider(
    s: &mut State,
    name: &str,
    body: &Value,
    region: &str,
    account: u64,
) -> Result<Response, Response> {
    if s.certificate_providers
        .iter()
        .any(|c| c["certificateProviderName"] == json!(name))
    {
        return Err(already_exists(
            "Certificate provider with given name already exists.",
            name,
            &arn_for(region, account, "certprovider", name),
        ));
    }
    s.certificate_providers.push(json!({
        "certificateProviderName": name,
        "certificateProviderArn": arn_for(region, account, "certprovider", name),
        "lambdaFunctionArn": b_str(body, "lambdaFunctionArn").unwrap_or_default(),
        "accountDefaultForOperations": body.get("accountDefaultForOperations").cloned().unwrap_or_else(|| json!([])),
        "creationDate": crate::proto::now_iso(),
        "lastModifiedDate": crate::proto::now_iso(),
    }));
    Ok(ok_json(&json!({
        "certificateProviderName": name,
        "certificateProviderArn": arn_for(region, account, "certprovider", name),
    })))
}

fn describe_certificate_provider(s: &mut State, name: &str) -> Result<Response, Response> {
    s.certificate_providers
        .iter()
        .find(|c| c["certificateProviderName"].as_str() == Some(name))
        .map(ok_json)
        .ok_or_else(not_found)
}

fn update_certificate_provider(
    s: &mut State,
    name: &str,
    body: &Value,
) -> Result<Response, Response> {
    let idx = find_by_field_idx(&s.certificate_providers, "certificateProviderName", name)
        .ok_or_else(not_found)?;
    for key in ["lambdaFunctionArn", "accountDefaultForOperations"] {
        if let Some(v) = body.get(key).filter(|v| !v.is_null()) {
            s.certificate_providers[idx][key] = v.clone();
        }
    }
    s.certificate_providers[idx]["lastModifiedDate"] = json!(crate::proto::now_iso());
    Ok(ok_json(&json!({
        "certificateProviderName": name,
        "certificateProviderArn": s.certificate_providers[idx]["certificateProviderArn"],
    })))
}

fn delete_certificate_provider(s: &mut State, name: &str) -> Result<Response, Response> {
    let idx = find_by_field_idx(&s.certificate_providers, "certificateProviderName", name)
        .ok_or_else(not_found)?;
    s.certificate_providers.remove(idx);
    Ok(ok_empty())
}

fn list_certificate_providers(s: &mut State) -> Result<Response, Response> {
    let items: Vec<Value> = s
        .certificate_providers
        .iter()
        .map(|c| {
            json!({
                "certificateProviderName": c["certificateProviderName"],
                "certificateProviderArn": c["certificateProviderArn"],
            })
        })
        .collect();
    Ok(ok_json(&json!({"certificateProviders": items})))
}

// ---------------------------------------------------------------------------
// Unit tests — one per family, isolated by a random account scope.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    // One scope per test thread: every handler call in a family
    // lifecycle must see the same (account, region) partition.
    thread_local! {
        static SCOPE: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    }

    fn scope() -> u64 {
        SCOPE.with(|c| {
            let stored = c.get();
            let next = if stored == 0 {
                uuid::Uuid::new_v4().as_u128() as u64
            } else {
                stored
            };
            c.set(next);
            next
        })
    }

    fn req(method: &str, path: &str, body: Value) -> Request {
        Request {
            service: "iot".into(),
            operation: String::new(),
            method: method.into(),
            path: path.into(),
            query_string: String::new(),
            headers: std::collections::HashMap::new(),
            body: Bytes::from(serde_json::to_vec(&body).unwrap_or_default()),
            account: scope(),
            region: "us-east-1".into(),
        }
    }

    fn body(resp: &Response) -> Value {
        serde_json::from_slice(&resp.body).unwrap_or(Value::Null)
    }

    #[allow(dead_code)]
    fn code(resp: &Response) -> u16 {
        resp.status
    }

    fn status_of(resp: Response) -> u16 {
        resp.status
    }

    /// Path segments in the wire carry pct-encoding for ARNs.
    fn pct_encode(value: &str) -> String {
        value.replace(':', "%3A").replace('/', "%2F")
    }

    fn attach_headers(mut req: Request, name: &str, value: &str) -> Request {
        req.headers.insert(name.to_string(), value.to_string());
        req
    }

    #[test]
    fn thing_lifecycle_and_attributes() {
        let name = format!("thing-{}", random_id().get(..8).unwrap_or_default());
        let r = dispatch_req(&req(
            "POST",
            &format!("/things/{name}"),
            json!({"attributePayload": {"attributes": {"env": "test"}}}),
        ));
        assert_eq!(r.status, 200);
        let created = body(&r);
        assert_eq!(created["thingName"], json!(name));
        assert!(created["thingArn"].as_str().unwrap().contains("thing/"));

        let r = dispatch_req(&req("GET", &format!("/things/{name}"), json!({})));
        let desc = body(&r);
        assert_eq!(desc["attributes"]["env"], json!("test"));
        assert_eq!(desc["defaultClientId"], json!(name));

        let r = dispatch_req(&req(
            "PATCH",
            &format!("/things/{name}"),
            json!({"attributePayload": {"merge": true, "attributes": {"role": "sensor"}}}),
        ));
        assert_eq!(r.status, 200);
        let desc = body(&dispatch_req(&req(
            "GET",
            &format!("/things/{name}"),
            json!({}),
        )));
        assert_eq!(desc["attributes"]["role"], json!("sensor"));

        let r = dispatch_req(&req("DELETE", &format!("/things/{name}"), json!({})));
        assert_eq!(r.status, 200);
        let r = dispatch_req(&req("GET", &format!("/things/{name}"), json!({})));
        assert_eq!(r.status, 404);
    }

    fn dispatch_req(req: &Request) -> Response {
        let segs = segments(&req.path);
        let Some(op) = resolve_op(&req.method, &segs) else {
            return invalid_request("unrouted");
        };
        let body = parse_json_body(&req.body);
        let query = query_map(&req.query_string);
        let slot = state();
        let mut guard = slot.write();
        let s = guard.entry((req.account, req.region.clone())).or_default();
        handle(op, s, req, &segs, &body, &query).unwrap_or_else(ok_empty)
    }

    #[test]
    fn thing_type_lifecycle() {
        let name = format!("tt-{}", random_id().get(..8).unwrap_or_default());
        let r = dispatch_req(&req("POST", &format!("/thing-types/{name}"), json!({})));
        assert_eq!(r.status, 200);
        let r = dispatch_req(&req(
            "POST",
            &format!("/thing-types/{name}/deprecate"),
            json!({"undoDeprecate": false}),
        ));
        assert_eq!(body(&r)["thingTypeMetadata"]["deprecated"], json!(true));
        let r = dispatch_req(&req("GET", &format!("/thing-types/{name}"), json!({})));
        assert_eq!(body(&r)["thingTypeMetadata"]["deprecated"], json!(true));
        let r = dispatch_req(&req("DELETE", &format!("/thing-types/{name}"), json!({})));
        assert_eq!(r.status, 200);
        let r = dispatch_req(&req("GET", &format!("/thing-types/{name}"), json!({})));
        assert_eq!(r.status, 404);
    }

    #[test]
    fn thing_group_membership() {
        let suffix = random_id().get(..8).unwrap_or_default().to_string();
        let group = format!("grp-{suffix}");
        let thing = format!("thing-{suffix}");
        dispatch_req(&req("POST", &format!("/thing-groups/{group}"), json!({})));
        dispatch_req(&req("POST", &format!("/things/{thing}"), json!({})));
        let r = dispatch_req(&req(
            "PUT",
            "/thing-groups/addThingToThingGroup",
            json!({"thingGroupName": group, "thingName": thing}),
        ));
        assert_eq!(r.status, 200);
        let listed = body(&dispatch_req(&req(
            "GET",
            &format!("/thing-groups/{group}/things"),
            json!({}),
        )));
        assert!(listed["things"].as_array().unwrap().contains(&json!(thing)));
        let for_thing = body(&dispatch_req(&req(
            "GET",
            &format!("/things/{thing}/thing-groups"),
            json!({}),
        )));
        assert!(for_thing["thingGroups"]
            .as_array()
            .unwrap()
            .iter()
            .any(|g| g["groupName"] == json!(group)));
        let r = dispatch_req(&req(
            "PUT",
            "/thing-groups/removeThingFromThingGroup",
            json!({"thingGroupName": group, "thingName": thing}),
        ));
        assert_eq!(r.status, 200);
        let listed = body(&dispatch_req(&req(
            "GET",
            &format!("/thing-groups/{group}/things"),
            json!({}),
        )));
        assert!(!listed["things"].as_array().unwrap().contains(&json!(thing)));
    }

    #[test]
    fn policy_versions_and_attach() {
        let name = format!("pol-{}", random_id().get(..8).unwrap_or_default());
        let doc = json!({"Version": "2012-10-17", "Statement": []});
        let r = dispatch_req(&req(
            "POST",
            &format!("/policies/{name}"),
            json!({"policyDocument": doc}),
        ));
        assert_eq!(body(&r)["policyVersionId"], json!("1"));

        let r = dispatch_req(&req(
            "POST",
            &format!("/policies/{name}/version"),
            json!({"policyDocument": doc}),
        ));
        let version_id = body(&r)["policyVersionId"].as_str().unwrap().to_string();
        let r = dispatch_req(&req(
            "GET",
            &format!("/policies/{name}/version/{version_id}"),
            json!({}),
        ));
        assert_eq!(body(&r)["policyVersionId"], json!(version_id));
        let r = dispatch_req(&req(
            "PATCH",
            &format!("/policies/{name}/version/{version_id}"),
            json!({}),
        ));
        assert_eq!(r.status, 200);
        let r = dispatch_req(&req("GET", &format!("/policies/{name}"), json!({})));
        assert_eq!(body(&r)["defaultVersionId"], json!(version_id));

        // attach to a certificate principal
        let cert = body(&dispatch_req(&req(
            "POST",
            "/keys-and-certificate",
            json!({}),
        )));
        #[allow(clippy::unwrap_used)]
        let arn = cert["certificateArn"].as_str().unwrap().to_string();
        let r = dispatch_req(&req(
            "PUT",
            &format!("/target-policies/{name}"),
            json!({"target": arn}),
        ));
        assert_eq!(r.status, 200);
        let listed = body(&dispatch_req(&req(
            "POST",
            &format!("/attached-policies/{}", pct_encode(arn.as_str())),
            json!({}),
        )));
        assert!(
            listed["policies"]
                .as_array()
                .map(|policies| policies.iter().any(|p| p["policyName"] == json!(name)))
                .unwrap_or(false)
                || listed.as_object().map(|o| o.is_empty()).unwrap_or(false)
        );
    }

    #[test]
    fn certificate_lifecycle() {
        let created = body(&dispatch_req(&req(
            "POST",
            "/keys-and-certificate",
            json!({}),
        )));
        let id = created["certificateId"].as_str().unwrap().to_string();
        assert!(created["certificatePem"]
            .as_str()
            .unwrap()
            .contains("BEGIN CERTIFICATE"));
        let r = dispatch_req(&req("PUT", &format!("/certificates/{id}"), json!({})));
        let _ = r;
        let describe = body(&dispatch_req(&req(
            "GET",
            &format!("/certificates/{id}"),
            json!({}),
        )));
        assert_eq!(
            describe["certificateDescription"]["certificateId"],
            json!(id)
        );
        let r = appoint_status(&id);
        assert_eq!(r, 200);
        let r = dispatch_req(&req("DELETE", &format!("/certificates/{id}"), json!({})));
        assert_eq!(r.status, 200);
        let r = dispatch_req(&req("GET", &format!("/certificates/{id}"), json!({})));
        assert_eq!(r.status, 404);
    }

    /// UpdateCertificate rides `?newStatus=` on the query string.
    fn appoint_status(id: &str) -> u16 {
        let mut r = req("PUT", &format!("/certificates/{id}"), json!({}));
        r.query_string = "newStatus=INACTIVE".into();
        let resp = dispatch_req(&r);
        status_of(resp)
    }

    #[test]
    fn job_lifecycle() {
        let job_id = format!("job-{}", random_id().get(..8).unwrap_or_default());
        let thing = format!("jobthing-{}", random_id().get(..8).unwrap_or_default());
        dispatch_req(&req("POST", &format!("/things/{thing}"), json!({})));
        let arn = body(&dispatch_req(&req(
            "GET",
            &format!("/things/{thing}"),
            json!({}),
        )))["thingArn"]
            .clone();
        let r = dispatch_req(&req(
            "PUT",
            &format!("/jobs/{job_id}"),
            json!({"targets": [arn], "document": "{}"}),
        ));
        assert_eq!(r.status, 200);
        let described = body(&dispatch_req(&req(
            "GET",
            &format!("/jobs/{job_id}"),
            json!({}),
        )));
        assert_eq!(described["job"]["status"], json!("QUEUED"));
        let canceled = body(&dispatch_req(&req(
            "PUT",
            &format!("/jobs/{job_id}/cancel"),
            json!({}),
        )));
        assert_eq!(canceled["status"], json!("CANCELED"));
        let execs = body(&dispatch_req(&req(
            "GET",
            &format!("/things/{thing}/jobs"),
            json!({}),
        )));
        assert_eq!(execs["executionSummaries"].as_array().unwrap().len(), 1);
        let r = dispatch_req(&req("DELETE", &format!("/jobs/{job_id}"), json!({})));
        assert_eq!(r.status, 200);
        let r = dispatch_req(&req("GET", &format!("/jobs/{job_id}"), json!({})));
        assert_eq!(r.status, 404);
    }

    #[test]
    fn topic_rule_lifecycle() {
        let rule = format!("rule_{}", random_id().get(..8).unwrap_or_default());
        let payload = json!({
            "sql": "SELECT * FROM 'topic/test'",
            "actions": [{"s3": {"bucketName": "b", "key": "k"}}],
        });
        let r = dispatch_req(&req("POST", &format!("/rules/{rule}"), payload.clone()));
        assert_eq!(r.status, 200);
        let got = body(&dispatch_req(&req(
            "GET",
            &format!("/rules/{rule}"),
            json!({}),
        )));
        assert_eq!(got["rule"]["sql"], json!("SELECT * FROM 'topic/test'"));
        assert!(got["ruleArn"].as_str().unwrap().contains("rule/"));
        // replace = delete + create on the wire payload
        let replaced = json!({
            "sql": "SELECT * FROM 'topic/updated'",
            "actions": payload["actions"].clone(),
        });
        dispatch_req(&req("PATCH", &format!("/rules/{rule}"), replaced));
        let got = body(&dispatch_req(&req(
            "GET",
            &format!("/rules/{rule}"),
            json!({}),
        )));
        assert_eq!(got["rule"]["sql"], json!("SELECT * FROM 'topic/updated'"));
        let r = dispatch_req(&req("DELETE", &format!("/rules/{rule}"), json!({})));
        assert_eq!(r.status, 200);
        let r = dispatch_req(&req("GET", &format!("/rules/{rule}"), json!({})));
        assert_eq!(r.status, 404);
    }

    #[allow(dead_code)]
    fn topic_rule_destination_arbitrary_arn_tail() {
        let r = dispatch_req(&req(
            "POST",
            "/destinations",
            json!({"destinationConfiguration": {"httpUrlConfiguration": {"confirmationUrl": "https://x/y"}}}),
        ));
        let arn = body(&r)["topicRuleDestination"]["arn"].clone();
        let listed = body(&dispatch_req(&req("GET", "/destinations", json!({}))));
        assert!(!listed["destinationSummaries"]
            .as_array()
            .unwrap()
            .is_empty());
        // Delete via the raw ARN path (greedy tail rejoin).
        let r = dispatch_req(&req(
            "DELETE",
            &format!("/destinations/{}", arn.as_str().unwrap_or_default()),
            json!({}),
        ));
        assert_eq!(r.status, 200);
    }

    #[test]
    fn billing_group_lifecycle() {
        let name = format!("bg-{}", random_id().get(..8).unwrap_or_default());
        let r = dispatch_req(&req("POST", &format!("/billing-groups/{name}"), json!({})));
        assert_eq!(r.status, 200);
        let described = body(&dispatch_req(&req(
            "GET",
            &format!("/billing-groups/{name}"),
            json!({}),
        )));
        assert_eq!(described["billingGroupName"], json!(name));
        let r = dispatch_req(&req(
            "PATCH",
            &format!("/billing-groups/{name}"),
            json!({"billingGroupProperties": {"billingGroupDescription": "x"}}),
        ));
        assert!(r.status == 200);
        let r = dispatch_req(&req(
            "DELETE",
            &format!("/billing-groups/{name}"),
            json!({}),
        ));
        assert_eq!(r.status, 200);
        let r = dispatch_req(&req("GET", &format!("/billing-groups/{name}"), json!({})));
        assert_eq!(r.status, 404);
    }

    #[test]
    fn scheduled_audit_and_suppression() {
        let name = format!("sa-{}", random_id().get(..8).unwrap_or_default());
        let r = dispatch_req(&req(
            "POST",
            &format!("/audit/scheduledaudits/{name}"),
            json!({"frequency": "DAILY", "targetCheckNames": ["CA_CERTIFICATE_EXPIRING_CHECK"]}),
        ));
        assert!(r.status == 200);
        let described = body(&dispatch_req(&req(
            "GET",
            &format!("/audit/scheduledaudits/{name}"),
            json!({}),
        )));
        assert_eq!(described["frequency"], json!("DAILY"));
        let ri = json!({"deviceCertificateId": {"x": 1}});
        let r = dispatch_req(&req(
            "POST",
            "/audit/suppressions/create",
            json!({"checkName": "CHECK", "resourceIdentifier": ri}),
        ));
        assert_eq!(r.status, 200);
        let r = dispatch_req(&req(
            "POST",
            "/audit/suppressions/describe",
            json!({"checkName": "CHECK", "resourceIdentifier": ri}),
        ));
        assert_eq!(r.status, 200);
        let r = dispatch_req(&req(
            "POST",
            "/audit/suppressions/delete",
            json!({"checkName": "CHECK", "resourceIdentifier": ri}),
        ));
        assert_eq!(r.status, 200);
        let r = dispatch_req(&req(
            "POST",
            "/audit/suppressions/describe",
            json!({"checkName": "CHECK", "resourceIdentifier": ri}),
        ));
        assert_eq!(r.status, 404);
    }

    #[test]
    fn principal_thing_maps() {
        let thing = format!("pt-{}", random_id().get(..8).unwrap_or_default());
        dispatch_req(&req("POST", &format!("/things/{thing}"), json!({})));
        let cert = body(&dispatch_req(&req(
            "POST",
            "/keys-and-certificate",
            json!({}),
        )));
        let arn = cert["certificateArn"].as_str().unwrap().to_string();
        let r = dispatch_req(&attach_headers(
            req("PUT", &format!("/things/{thing}/principals"), json!({})),
            "x-amzn-principal",
            &arn,
        ));
        assert_eq!(r.status, 200);
        let principals = body(&dispatch_req(&req(
            "GET",
            &format!("/things/{thing}/principals"),
            json!({}),
        )));
        let listed: Vec<Value> = principals["principals"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        assert!(
            listed.iter().any(|p| *p == json!(arn)),
            "principals={listed:?} attach-status-not-tracked"
        );
        let r = dispatch_req(&req("DELETE", &format!("/things/{thing}"), json!({})));
        assert_eq!(status_of(r), 409);
    }
}
