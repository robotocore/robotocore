//! SSO Admin backend (JSON protocol, target `SWBExternalService.<Op>`).
//!
//! Port of moto's ssoadmin: pre-seeded SSO instance per account/region,
//! permission sets and account assignments.
//!
//! moto fork quirks kept verbatim: `UpdatePermissionSet` overwrites the stored
//! description/relay state with nulls when the client omits them and resets
//! `SessionDuration` to 3600 (moto reads the params with those defaults);
//! `ListPermissionSetsProvisionedToAccount` reads the instance provisioning
//! record rather than every permission set; managed-policy attachments are
//! validated against moto's `aws_managed_policies` dataset (embedded as
//! `ssoadmin_data/aws_managed_policies.json`) and both managed and
//! customer-managed attachments share the same limit of 20 per permission set;
//! `DeleteAccountAssignment` raises `ResourceNotFoundException` when nothing
//! matched and the deleted assignment keeps its request id for the
//! `DescribeAccountAssignmentDeletionStatus` lookup.
//!
//! The application-era surface (instances beyond the seed, region
//! replication, applications and their assignment/access/auth/grant/session
//! configs, trusted token issuers, ABAC configurations, permissions
//! boundaries and the operation-status lists) has no handler in the moto
//! fork; its shapes are pinned to the botocore wire model (sso-admin
//! 2020-07-20). Decisions taken there: missing-ARN lookups answer the
//! module's modeled `ResourceNotFoundException` (moto's JsonRESTError 400
//! envelope) even where botocore does not declare that error; update ops
//! write only members present on the request body (no moto mimicry exists
//! for them); `DeleteInstance` cascades to everything anchored on the
//! instance (regions, ABAC config, applications, assignments, issuers and
//! provisioning statuses).

use crate::store;
use crate::{Request, Response};
use base64::Engine as _;
use once_cell::sync::Lazy;
use serde_json::{json, Value};
use std::collections::HashMap;

const MAX_MANAGED_POLICIES_PER_PERMISSION_SET: i64 = 20;

#[derive(Default)]
struct State {
    /// Seeded lazily on first use, exactly one instance like moto.
    instances: Vec<Value>,
    /// Permission sets in moto's list order (moto re-appends on update).
    permission_sets: Vec<PsRecord>,
    /// Active assignments, moto's `AccountAssignment` shape.
    assignments: Vec<Assignment>,
    /// moto's `deleted_account_assignments`, for the DeletionStatus lookup.
    deleted_assignments: Vec<Assignment>,
    /// moto's `Instance.provisioned_permission_sets`: instance arn -> ps arns.
    provisioned: Vec<(String, Vec<String>)>,
    /// Untagged-but-recorded tags: arn -> {Key: Value}.
    tags: HashMap<String, Value>,
    /// ProvisionPermissionSet records, for the botocore status lookups.
    provisioning_statuses: Vec<ProvisioningStatus>,
    /// Region-replication records; the primary region is the caller's
    /// region and seeds lazily.
    regions: Vec<Value>,
    /// instance arn -> the ABAC configuration body.
    instance_abac: Vec<(String, Value)>,
    /// Applications created through the application-era arms.
    applications: Vec<AppRecord>,
    /// Principal assignments on applications.
    app_assignments: Vec<AppAssignment>,
    /// Trusted token issuer configurations.
    token_issuers: Vec<TtiRecord>,
}

/// The application-era record: the wire `Application` body (the
/// DescribeApplication member and ListApplications item) plus the config
/// collections the application-era ops attach to it.
struct AppRecord {
    instance_arn: String,
    body: Value,
    /// AssignmentRequired; real AWS defaults to true.
    assignment_required: bool,
    /// (Scope, AuthorizedTargets).
    access_scopes: Vec<(String, Value)>,
    /// (AuthenticationMethodType, AuthenticationMethod body).
    auth_methods: Vec<(String, Value)>,
    /// (GrantType, Grant body).
    grants: Vec<(String, Value)>,
    /// The session-configuration status; real AWS defaults to DISABLED.
    background_status: Value,
}

/// moto-era `AssignmentSummary`-style record for application assignments.
struct AppAssignment {
    application_arn: String,
    principal_id: Value,
    principal_type: Value,
}

/// The trusted token issuer record: the DescribeTrustedTokenIssuer body
/// plus the instance it belongs to.
struct TtiRecord {
    arn: String,
    instance_arn: String,
    body: Value,
}

/// ProvisionPermissionSet's status record: moto's handler builds the wire
/// body fresh without storing it; the wave keeps the record so the
/// botocore status lookups (`DescribePermissionSetProvisioningStatus`,
/// `ListPermissionSetProvisioningStatus`) resolve afterwards.
struct ProvisioningStatus {
    request_id: String,
    instance_arn: String,
    body: Value,
}

/// moto's `PermissionSet` record: the wire summary body plus the mutable
/// policy state the fork tracks on the object.
struct PsRecord {
    instance_arn: String,
    /// Wire summary (Name/Description/PermissionSetArn/SessionDuration/
    /// RelayState/CreatedDate) exactly as moto's to_json renders it.
    body: Value,
    /// moto's `inline_policy` — stored raw (absent `InlinePolicy` reads null).
    inline_policy: Value,
    /// (arn, name) — moto's ManagedPolicy entries.
    managed_policies: Vec<(String, String)>,
    /// (name, path) — moto's CustomerManagedPolicy entries; the path defaults
    /// to "/" and explicit-null paths never match (moto's dict semantics).
    customer_policies: Vec<(String, Option<String>)>,
    /// moto's `total_managed_policies_attached` (both policy kinds).
    attached_policies: i64,
    /// The PermissionsBoundary body; null until
    /// `PutPermissionsBoundaryToPermissionSet` runs.
    permissions_boundary: Value,
}

/// moto's `AccountAssignment` record.
struct Assignment {
    request_id: String,
    instance_arn: String,
    target_id: Value,
    target_type: Value,
    permission_set_arn: Value,
    principal_type: Value,
    principal_id: Value,
    created_date: f64,
}

fn state() -> store::Slot<State> {
    static SLOT: Lazy<store::Slot<State>> = Lazy::new(|| store::slot("ssoadmin"));
    SLOT.clone()
}

fn ok(v: &Value) -> Response {
    Response::json(200, serde_json::to_vec(v).unwrap_or_default())
}

/// moto's `utcnow().timestamp()` (float epoch seconds).
fn epoch() -> f64 {
    chrono::Utc::now().timestamp_millis() as f64 / 1000.0
}

/// moto's get_random_string: lowercase letters + digits.
fn random_string(len: usize) -> String {
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let bytes = uuid::Uuid::new_v4().as_bytes().to_vec();
    (0..len)
        .map(|i| ALPHABET[bytes[i % bytes.len()] as usize % ALPHABET.len()] as char)
        .collect()
}

fn ps_arn(instance_arn: &str) -> String {
    format!("{instance_arn}/ps-{}", random_string(16))
}

fn ps_json(
    name: Option<&str>,
    description: Option<&str>,
    arn: &str,
    session_duration: Value,
    relay_state: Value,
) -> Value {
    json!({
        "Name": name,
        "Description": description,
        "PermissionSetArn": arn,
        "SessionDuration": session_duration,
        "RelayState": relay_state,
    })
}

fn with_default_instance(st: &mut State, account: u64) {
    if st.instances.is_empty() {
        st.instances.push(json!({
            "CreatedDate": epoch(),
            "IdentityStoreId": format!("d-{}", random_string(10)),
            "InstanceArn": format!("arn:aws:sso:::instance/ssoins-{}", random_string(16)),
            "Name": Value::Null,
            "OwnerAccountId": account.to_string(),
            "Status": "ACTIVE",
        }));
    }
}

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "ssoadmin" {
        return None;
    }
    let body = crate::proto::parse_json_body(&req.body);
    let slot = state();
    let mut st = slot.write();
    let b = st.entry((req.account, req.region.clone())).or_default();
    with_default_instance(b, req.account);
    Some(handle(b, &body, req))
}

/// moto's JsonRESTError: HTTP 400 + `{"__type": <error_type>, "message": ...}`.
fn error_response(error_type: &str, message: &str) -> Response {
    let e = json!({
        "__type": error_type,
        "message": message,
    });
    Response::json(400, serde_json::to_vec(&e).unwrap_or_default())
}

fn rnf(message: &str) -> Response {
    error_response("ResourceNotFoundException", message)
}

fn conflict(message: &str) -> Response {
    error_response("ConflictException", message)
}

fn quota_exceeded(message: &str) -> Response {
    error_response("ServiceQuotaExceededException", message)
}

/// moto's dict lookups that crash into a plain 500 (unreachable through boto3 —
/// the members are required shapes): mirror the 500 without the crash.
fn moto_internal_error() -> Response {
    let e = json!({
        "__type": "InternalError",
        "message": "InternalError",
    });
    Response::json(500, serde_json::to_vec(&e).unwrap_or_default())
}

/// moto's `_find_permission_set`: the tail of the ARN after the last "/".
fn ps_id(permission_set_arn: &str) -> String {
    arn_tail(permission_set_arn)
}

/// The ARN tail (`ssoins-…`, `apl-…`, `tti-…`): the id every not-found
/// message quotes, moto's `_find_*` convention.
fn arn_tail(arn: &str) -> String {
    arn.split('/').next_back().unwrap_or("").to_string()
}

fn find_instance(st: &State, instance_arn: &str) -> Option<usize> {
    st.instances
        .iter()
        .position(|i| i["InstanceArn"] == json!(instance_arn))
}

/// The modeled missing-instance envelope (botocore declares
/// `ResourceNotFoundException` on the ops that take an InstanceArn).
fn instance_not_found(instance_arn: &str) -> Response {
    rnf(&format!(
        "Could not find Instance with id {}",
        arn_tail(instance_arn)
    ))
}

/// Region-replication records seed lazily: the primary region is the
/// caller's region (state partitions are per (account, region)).
fn ensure_primary_region(st: &mut State, region: &str) {
    if st.regions.is_empty() {
        st.regions.push(json!({
            "RegionName": region,
            "Status": "ACTIVE",
            "AddedDate": epoch(),
            "IsPrimaryRegion": true,
        }));
    }
}

fn find_application(st: &State, application_arn: &str) -> Option<usize> {
    st.applications
        .iter()
        .position(|a| a.body["ApplicationArn"] == json!(application_arn))
}

fn application_not_found(application_arn: &str) -> Response {
    rnf(&format!(
        "Could not find Application with id {}",
        ps_id(application_arn)
    ))
}

fn find_ps(st: &State, instance_arn: &str, permission_set_arn: &str) -> Option<usize> {
    st.permission_sets.iter().position(|p| {
        p.instance_arn == instance_arn && p.body["PermissionSetArn"] == json!(permission_set_arn)
    })
}

/// moto's `_find_permission_set` miss message.
fn ps_not_found(permission_set_arn: &str) -> Response {
    rnf(&format!(
        "Could not find PermissionSet with id {}",
        ps_id(permission_set_arn)
    ))
}

/// moto's `Instance.provisioned_permission_sets` for `instance_arn`.
fn provisioned_for<'a>(st: &'a State, instance_arn: &str) -> Option<&'a Vec<String>> {
    st.provisioned
        .iter()
        .find(|(i, _)| i == instance_arn)
        .map(|(_, list)| list)
}

/// moto's `get_partition` for `_find_managed_policy`'s expected ARN.
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

/// moto's `_find_managed_policy`: resolve the ARN against the managed-policy
/// dataset, requiring the exact `arn:<partition>:iam::aws:policy<path><name>`.
/// Returns the policy name on match.
fn find_managed_policy(region: &str, managed_policy_arn: &str) -> Option<String> {
    static POLICIES: Lazy<HashMap<String, String>> = Lazy::new(|| {
        // name -> path, from moto's iam `aws_managed_policies` dataset.
        serde_json::from_str(include_str!("ssoadmin_data/aws_managed_policies.json"))
            .unwrap_or_default()
    });
    let policy_name = managed_policy_arn.split('/').next_back().unwrap_or("");
    let path = POLICIES.get(policy_name)?;
    let expected = format!(
        "arn:{}:iam::aws:policy{path}{policy_name}",
        partition(region)
    );
    (managed_policy_arn == expected).then(|| policy_name.to_string())
}

/// Read a body member the way moto does: absent key == null.
fn param(body: &Value, key: &str) -> Value {
    body.get(key).cloned().unwrap_or(Value::Null)
}

/// String member read: absent key reads "".
fn body_str<'a>(body: &'a Value, key: &str) -> &'a str {
    body.get(key).and_then(Value::as_str).unwrap_or("")
}

/// `CustomerManagedPolicyReference.Path`: absent key defaults to "/", an
/// explicit null or non-string never matches (moto's `.get("Path", "/")`).
fn reference_path(reference: &Value) -> Option<String> {
    match reference.get("Path") {
        None => Some("/".to_string()),
        Some(Value::String(s)) => Some(s.clone()),
        Some(_) => None,
    }
}

/// Python f-string rendering of the optional path in error messages.
fn wire_str(v: Option<&str>) -> String {
    match v {
        Some(s) => s.to_string(),
        None => "None".to_string(),
    }
}

/// moto's `AccountAssignment.to_json(include_creation_date,
/// include_request_id)` plus the handler-injected Status.
fn assignment_json(a: &Assignment, status: Option<&str>) -> Value {
    let mut summary = json!({
        "TargetId": a.target_id,
        "TargetType": a.target_type,
        "PermissionSetArn": a.permission_set_arn,
        "PrincipalType": a.principal_type,
        "PrincipalId": a.principal_id,
        "CreatedDate": a.created_date,
        "RequestId": a.request_id,
    });
    if let Some(s) = status {
        summary["Status"] = json!(s);
    }
    summary
}

/// Application-era updates write only the members present on the request
/// body (no moto fork exists to mimic there).
fn put_present(body: &mut Value, p: &Value, keys: &[&str]) {
    if let Some(obj) = body.as_object_mut() {
        for key in keys {
            if let Some(v) = p.get(*key) {
                obj.insert((*key).to_string(), v.clone());
            }
        }
    }
}

/// The ApplicationAssignment wire body (create input echo, the
/// DescribeApplicationAssignment member, list items).
fn app_assignment_json(x: &AppAssignment) -> Value {
    json!({
        "ApplicationArn": x.application_arn,
        "PrincipalId": x.principal_id,
        "PrincipalType": x.principal_type,
    })
}

/// DeleteInstance's cascade: regions, ABAC config, applications,
/// assignments, token issuers and provisioning records are anchored on
/// the instance.
fn drop_instance_state(st: &mut State, instance_arn: &str) {
    st.regions.clear();
    st.instance_abac.retain(|(a, _)| a != instance_arn);
    let arns: Vec<String> = st
        .applications
        .iter()
        .filter(|a| a.instance_arn == instance_arn)
        .filter_map(|a| a.body["ApplicationArn"].as_str())
        .map(str::to_string)
        .collect();
    st.applications.retain(|a| a.instance_arn != instance_arn);
    st.app_assignments
        .retain(|x| !arns.contains(&x.application_arn));
    st.token_issuers.retain(|t| t.instance_arn != instance_arn);
    st.provisioning_statuses
        .retain(|s| s.instance_arn != instance_arn);
    st.instances
        .retain(|i| i["InstanceArn"] != json!(instance_arn));
}

/// An OperationStatusFilter on a status list: moto-era records are always
/// SUCCEEDED, so only that value survives a non-empty filter.
fn status_filter(p: &Value) -> impl Fn() -> bool + '_ {
    let wanted = p
        .get("Filter")
        .and_then(|f| f.get("Status"))
        .and_then(Value::as_str)
        .unwrap_or("");
    move || wanted.is_empty() || wanted == "SUCCEEDED"
}

/// moto's `paginate` semantics (unique_attribute-driven, limit_default 100):
/// slice `[start, start + max_results)` off the ordered list; emit a next
/// token describing the first item AFTER the page when more remain.
/// Each caller keys its item with python-`str()` renders of the operation's
/// `unique_attribute` members joined with `|` (see `paginated_key`).
fn paginate_keyed(
    items: Vec<(String, Value)>,
    max_results: Option<i64>,
    starting_token: Option<&str>,
) -> Result<(Vec<Value>, Value), Response> {
    let limit = match max_results {
        Some(n) if n > 0 => n as usize,
        // moto: falsy limit falls back to limit_default 100.
        _ => 100,
    };
    let start = match starting_token {
        None => 0usize,
        Some(token) => {
            let predicate = decode_resume_token(token)?;
            items
                .iter()
                .position(|(key, _)| *key == predicate)
                .ok_or_else(|| invalid_token("Resource not found!"))?
        }
    };
    let end = (start + limit).min(items.len());
    let page: Vec<Value> = items[start..end].iter().map(|(_, v)| v.clone()).collect();
    let next = if !page.is_empty() && end < items.len() {
        Value::String(encode_resume_token(&[items[end].0.clone()]))
    } else {
        Value::Null
    };
    Ok((page, next))
}

/// python-`str()` of a wire member for the resume-token key: strings pass
/// through, everything else renders like moto's `str()` (null members
/// become `None`).
fn py_str(v: &Value) -> String {
    match v.as_str() {
        Some(s) => s.to_string(),
        None => match v {
            Value::Null => "None".to_string(),
            Value::Bool(b) => b.to_string(),
            Value::Number(n) => n.to_string(),
            other => other.to_string(),
        },
    }
}

/// The resume-token key for a paginated wire item: the operation's
/// `unique_attribute` values rendered like python's `getattr`-based token
/// builder (`"|".join(str(item[attr]) ...)`).
fn paginated_key(item: &Value, attrs: &[&str]) -> String {
    attrs
        .iter()
        .map(|attr| py_str(item.get(*attr).unwrap_or(&Value::Null)))
        .collect::<Vec<_>>()
        .join("|")
}

/// base64 of `{"uniqueAttributes": "a|b"}` — moto's `TokenEncoder` output
/// without the parameterChecksum (the fork only adds it when input
/// parameters exist, and its decoder tolerates the missing checksum).
fn encode_resume_token(parts: &[String]) -> String {
    let doc = json!({ "uniqueAttributes": parts.join("|") });
    base64::engine::general_purpose::STANDARD.encode(serde_json::to_vec(&doc).unwrap_or_default())
}

/// base64(JSON) -> the joined `uniqueAttributes` string.
fn decode_resume_token(token: &str) -> Result<String, Response> {
    base64::engine::general_purpose::STANDARD
        .decode(token.as_bytes())
        .ok()
        .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
        .and_then(|value| {
            value
                .get("uniqueAttributes")
                .and_then(Value::as_str)
                .map(String::from)
        })
        .ok_or_else(|| invalid_token("Invalid token"))
}

fn max_results(p: &Value) -> Option<i64> {
    p.get("MaxResults").and_then(Value::as_i64)
}

fn next_token(p: &Value) -> Option<&str> {
    p.get("NextToken").and_then(Value::as_str)
}

/// moto's `InvalidToken` (an `AWSError`, code 400).
fn invalid_token(message: &str) -> Response {
    error_response("InvalidToken", &format!("Invalid Token: {message}"))
}

fn handle(st: &mut State, p: &Value, req: &Request) -> Response {
    match req.operation.as_str() {
        "ListInstances" => ok(&json!({"Instances": st.instances})),
        "UpdateInstance" => {
            // moto loops and updates matching instances only; an unknown ARN
            // is a silent no-op.
            let arn = param(p, "InstanceArn");
            let name = param(p, "Name");
            for instance in &mut st.instances {
                if instance["InstanceArn"] == arn {
                    instance["Name"] = name.clone();
                }
            }
            ok(&json!({}))
        }
        "CreatePermissionSet" => {
            let instance_arn = body_str(p, "InstanceArn").to_string();
            let arn = ps_arn(&instance_arn);
            let mut body = ps_json(
                p.get("Name").and_then(Value::as_str),
                p.get("Description").and_then(Value::as_str),
                &arn,
                // moto reads SessionDuration with a 3600 default (an int,
                // not a string, when the member is absent).
                p.get("SessionDuration")
                    .cloned()
                    .unwrap_or_else(|| json!(3600)),
                param(p, "RelayState"),
            );
            body["CreatedDate"] = json!(epoch());
            if let Some(tags) = p.get("Tags") {
                store_tags(st, &arn, tags);
            }
            st.permission_sets.push(PsRecord {
                instance_arn,
                body: body.clone(),
                // moto defaults the object's inline_policy to "".
                inline_policy: json!(""),
                managed_policies: Vec::new(),
                customer_policies: Vec::new(),
                attached_policies: 0,
                permissions_boundary: Value::Null,
            });
            ok(&json!({"PermissionSet": body}))
        }
        "DescribePermissionSet" => {
            let instance_arn = body_str(p, "InstanceArn");
            let ps = body_str(p, "PermissionSetArn");
            match find_ps(st, instance_arn, ps) {
                Some(i) => ok(&json!({"PermissionSet": st.permission_sets[i].body})),
                None => ps_not_found(ps),
            }
        }
        "ListPermissionSets" => {
            let instance_arn = body_str(p, "InstanceArn");
            let items: Vec<(String, Value)> = st
                .permission_sets
                .iter()
                .filter(|ps| ps.instance_arn == instance_arn)
                .map(|ps| {
                    let arn = ps.body["PermissionSetArn"].clone();
                    (arn.as_str().unwrap_or_default().to_string(), arn)
                })
                .collect();
            match paginate_keyed(items, max_results(p), next_token(p)) {
                Ok((page, next)) => ok(&json!({"PermissionSets": page, "NextToken": next})),
                Err(resp) => resp,
            }
        }
        "ListPermissionSetsProvisionedToAccount" => {
            // moto reads the instance provisioning record and ignores
            // AccountId/ProvisioningStatus/pagination.
            let instance_arn = body_str(p, "InstanceArn");
            let arns = provisioned_for(st, instance_arn)
                .map(|list| list.iter().map(|p| json!(p)).collect::<Vec<Value>>())
                .unwrap_or_default();
            ok(&json!({"PermissionSets": arns}))
        }
        "UpdatePermissionSet" => {
            let instance_arn = body_str(p, "InstanceArn");
            let ps = body_str(p, "PermissionSetArn");
            match find_ps(st, instance_arn, ps) {
                Some(i) => {
                    // moto overwrites unconditionally: absent Description and
                    // RelayState read null, SessionDuration defaults to 3600.
                    let rec = &mut st.permission_sets[i];
                    rec.body["Description"] = param(p, "Description");
                    rec.body["SessionDuration"] = p
                        .get("SessionDuration")
                        .cloned()
                        .unwrap_or_else(|| json!(3600));
                    rec.body["RelayState"] = param(p, "RelayState");
                    // moto removes the record and re-appends it.
                    let rec = st.permission_sets.remove(i);
                    st.permission_sets.push(rec);
                    ok(&json!({}))
                }
                None => ps_not_found(ps),
            }
        }
        "DeletePermissionSet" => {
            let instance_arn = body_str(p, "InstanceArn");
            let ps = body_str(p, "PermissionSetArn");
            match find_ps(st, instance_arn, ps) {
                Some(i) => {
                    st.permission_sets.remove(i);
                    for (_, list) in &mut st.provisioned {
                        list.retain(|a| a.as_str() != ps);
                    }
                    ok(&json!({}))
                }
                None => ps_not_found(ps),
            }
        }
        "PutInlinePolicyToPermissionSet" => {
            let instance_arn = body_str(p, "InstanceArn");
            let ps = body_str(p, "PermissionSetArn");
            match find_ps(st, instance_arn, ps) {
                Some(i) => {
                    st.permission_sets[i].inline_policy = param(p, "InlinePolicy");
                    ok(&json!({}))
                }
                None => ps_not_found(ps),
            }
        }
        "GetInlinePolicyForPermissionSet" => {
            let instance_arn = body_str(p, "InstanceArn");
            let ps = body_str(p, "PermissionSetArn");
            match find_ps(st, instance_arn, ps) {
                Some(i) => ok(&json!({"InlinePolicy": st.permission_sets[i].inline_policy})),
                None => ps_not_found(ps),
            }
        }
        "DeleteInlinePolicyFromPermissionSet" => {
            let instance_arn = body_str(p, "InstanceArn");
            let ps = body_str(p, "PermissionSetArn");
            match find_ps(st, instance_arn, ps) {
                Some(i) => {
                    st.permission_sets[i].inline_policy = json!("");
                    ok(&json!({}))
                }
                None => ps_not_found(ps),
            }
        }
        "AttachManagedPolicyToPermissionSet" => {
            let instance_arn = body_str(p, "InstanceArn");
            let ps = body_str(p, "PermissionSetArn");
            let policy_arn = body_str(p, "ManagedPolicyArn");
            match find_ps(st, instance_arn, ps) {
                Some(i) => match find_managed_policy(&req.region, policy_arn) {
                    Some(name) => {
                        let rec = &mut st.permission_sets[i];
                        if rec.managed_policies.iter().any(|(a, _)| a == policy_arn) {
                            return conflict(&format!(
                                "Permission set with id {} already has a typed link attachment to a manged policy with {policy_arn}",
                                ps_id(ps)
                            ));
                        }
                        if rec.attached_policies >= MAX_MANAGED_POLICIES_PER_PERMISSION_SET {
                            return quota_exceeded(&format!(
                                "You have exceeded AWS SSO limits. Cannot create ManagedPolicy more than {MAX_MANAGED_POLICIES_PER_PERMISSION_SET} for id {}. Please refer to https://docs.aws.amazon.com/singlesignon/latest/userguide/limits.html",
                                ps_id(ps)
                            ));
                        }
                        rec.managed_policies.push((policy_arn.to_string(), name));
                        rec.attached_policies += 1;
                        ok(&json!({}))
                    }
                    None => rnf(&format!("Policy does not exist with ARN: {policy_arn}")),
                },
                None => ps_not_found(ps),
            }
        }
        "ListManagedPoliciesInPermissionSet" => {
            let instance_arn = body_str(p, "InstanceArn");
            let ps = body_str(p, "PermissionSetArn");
            match find_ps(st, instance_arn, ps) {
                Some(i) => {
                    let items: Vec<(String, Value)> = st.permission_sets[i]
                        .managed_policies
                        .iter()
                        .map(|(arn, name)| {
                            let item = json!({"Arn": arn, "Name": name});
                            (arn.clone(), item)
                        })
                        .collect();
                    match paginate_keyed(items, max_results(p), next_token(p)) {
                        Ok((page, next)) => {
                            ok(&json!({"AttachedManagedPolicies": page, "NextToken": next}))
                        }
                        Err(resp) => resp,
                    }
                }
                None => ps_not_found(ps),
            }
        }
        "DetachManagedPolicyFromPermissionSet" => {
            let instance_arn = body_str(p, "InstanceArn");
            let ps = body_str(p, "PermissionSetArn");
            let policy_arn = body_str(p, "ManagedPolicyArn");
            match find_ps(st, instance_arn, ps) {
                Some(i) => {
                    let rec = &mut st.permission_sets[i];
                    match rec
                        .managed_policies
                        .iter()
                        .position(|(a, _)| a == policy_arn)
                    {
                        Some(j) => {
                            rec.managed_policies.remove(j);
                            rec.attached_policies -= 1;
                            ok(&json!({}))
                        }
                        None => rnf(&format!(
                            "Could not find ManagedPolicy with arn {policy_arn}"
                        )),
                    }
                }
                None => ps_not_found(ps),
            }
        }
        "AttachCustomerManagedPolicyReferenceToPermissionSet" => {
            let instance_arn = body_str(p, "InstanceArn");
            let ps = body_str(p, "PermissionSetArn");
            let reference = param(p, "CustomerManagedPolicyReference");
            match find_ps(st, instance_arn, ps) {
                Some(i) => {
                    if !reference.is_object()
                        || reference.get("Name").and_then(Value::as_str).is_none()
                    {
                        // moto indexes ["Name"] — a missing key is a plain 500.
                        return moto_internal_error();
                    }
                    let name = reference["Name"].as_str().unwrap_or("");
                    let path = reference_path(&reference);
                    let path_ref = path.as_deref();
                    let rec = &mut st.permission_sets[i];
                    if rec
                        .customer_policies
                        .iter()
                        .any(|(n, pt)| n == name && pt.as_deref() == path_ref)
                    {
                        return conflict(&format!(
                            "Given customer managed policy with name: {name}  and path {} already attached",
                            wire_str(path_ref)
                        ));
                    }
                    if rec.attached_policies >= MAX_MANAGED_POLICIES_PER_PERMISSION_SET {
                        return quota_exceeded(&format!(
                            "Cannot attach managed policy: number of attached managed policies is already at maximum {MAX_MANAGED_POLICIES_PER_PERMISSION_SET}"
                        ));
                    }
                    rec.customer_policies.push((name.to_string(), path));
                    rec.attached_policies += 1;
                    ok(&json!({}))
                }
                None => ps_not_found(ps),
            }
        }
        "ListCustomerManagedPolicyReferencesInPermissionSet" => {
            let instance_arn = body_str(p, "InstanceArn");
            let ps = body_str(p, "PermissionSetArn");
            match find_ps(st, instance_arn, ps) {
                Some(i) => {
                    let items: Vec<(String, Value)> = st.permission_sets[i]
                        .customer_policies
                        .iter()
                        .map(|(name, path)| {
                            let item = json!({"Name": name, "Path": path});
                            let key = format!("{name}|{}", wire_str(path.as_deref()));
                            (key, item)
                        })
                        .collect();
                    match paginate_keyed(items, max_results(p), next_token(p)) {
                        Ok((page, next)) => ok(&json!({
                            "CustomerManagedPolicyReferences": page,
                            "NextToken": next,
                        })),
                        Err(resp) => resp,
                    }
                }
                None => ps_not_found(ps),
            }
        }
        "DetachCustomerManagedPolicyReferenceFromPermissionSet" => {
            let instance_arn = body_str(p, "InstanceArn");
            let ps = body_str(p, "PermissionSetArn");
            let reference = param(p, "CustomerManagedPolicyReference");
            match find_ps(st, instance_arn, ps) {
                Some(i) => {
                    if !reference.is_object()
                        || reference.get("Name").and_then(Value::as_str).is_none()
                    {
                        return moto_internal_error();
                    }
                    let name = reference["Name"].as_str().unwrap_or("");
                    let path = reference_path(&reference);
                    let path_ref = path.as_deref();
                    let rec = &mut st.permission_sets[i];
                    match rec
                        .customer_policies
                        .iter()
                        .position(|(n, pt)| n == name && pt.as_deref() == path_ref)
                    {
                        Some(j) => {
                            rec.customer_policies.remove(j);
                            rec.attached_policies -= 1;
                            ok(&json!({}))
                        }
                        None => rnf(&format!(
                            "Given managed policy with name: {name}  and path {} does not exist on PermissionSet",
                            wire_str(path_ref)
                        )),
                    }
                }
                None => ps_not_found(ps),
            }
        }
        "DescribeAccountAssignmentCreationStatus" => {
            let request_id = body_str(p, "AccountAssignmentCreationRequestId");
            match st.assignments.iter().find(|a| a.request_id == request_id) {
                Some(a) => ok(&json!({
                    "AccountAssignmentCreationStatus": assignment_json(a, Some("SUCCEEDED"))
                })),
                None => rnf(""),
            }
        }
        "DescribeAccountAssignmentDeletionStatus" => {
            let request_id = body_str(p, "AccountAssignmentDeletionRequestId");
            match st
                .deleted_assignments
                .iter()
                .find(|a| a.request_id == request_id)
            {
                Some(a) => ok(&json!({
                    "AccountAssignmentDeletionStatus": assignment_json(a, Some("SUCCEEDED"))
                })),
                None => rnf(""),
            }
        }
        "CreateAccountAssignment" => {
            let assignment = Assignment {
                request_id: uuid::Uuid::new_v4().to_string(),
                instance_arn: body_str(p, "InstanceArn").to_string(),
                target_id: param(p, "TargetId"),
                target_type: param(p, "TargetType"),
                permission_set_arn: param(p, "PermissionSetArn"),
                principal_type: param(p, "PrincipalType"),
                principal_id: param(p, "PrincipalId"),
                created_date: epoch(),
            };
            let summary = assignment_json(&assignment, Some("SUCCEEDED"));
            st.assignments.push(assignment);
            ok(&json!({"AccountAssignmentCreationStatus": summary}))
        }
        "DeleteAccountAssignment" => {
            // moto matches all six parameters; a miss raises
            // ResourceNotFoundException with the empty default message.
            let instance_arn = body_str(p, "InstanceArn");
            let target_id = param(p, "TargetId");
            let target_type = param(p, "TargetType");
            let permission_set_arn = param(p, "PermissionSetArn");
            let principal_type = param(p, "PrincipalType");
            let principal_id = param(p, "PrincipalId");
            let idx = st.assignments.iter().position(|a| {
                a.instance_arn == instance_arn
                    && a.target_id == target_id
                    && a.target_type == target_type
                    && a.permission_set_arn == permission_set_arn
                    && a.principal_type == principal_type
                    && a.principal_id == principal_id
            });
            match idx {
                Some(i) => {
                    let assignment = st.assignments.remove(i);
                    let summary = assignment_json(&assignment, Some("SUCCEEDED"));
                    st.deleted_assignments.push(assignment);
                    ok(&json!({"AccountAssignmentDeletionStatus": summary}))
                }
                None => rnf(""),
            }
        }
        "ListAccountAssignments" => {
            let instance_arn = param(p, "InstanceArn");
            let account_id = param(p, "AccountId");
            let ps = param(p, "PermissionSetArn");
            let items: Vec<(String, Value)> = st
                .assignments
                .iter()
                // moto filters on all three: instance_arn, target_id and
                // permission_set_arn (a foreign instance yields nothing).
                .filter(|a| {
                    a.instance_arn == instance_arn.as_str().unwrap_or("")
                        && a.target_id == account_id
                        && a.permission_set_arn == ps
                })
                .map(|a| {
                    let item = json!({
                        "AccountId": a.target_id,
                        "PermissionSetArn": a.permission_set_arn,
                        "PrincipalType": a.principal_type,
                        "PrincipalId": a.principal_id,
                    });
                    (
                        paginated_key(
                            &item,
                            &[
                                "AccountId",
                                "PermissionSetArn",
                                "PrincipalId",
                                "PrincipalType",
                            ],
                        ),
                        item,
                    )
                })
                .collect();
            match paginate_keyed(items, max_results(p), next_token(p)) {
                Ok((page, next)) => ok(&json!({"AccountAssignments": page, "NextToken": next})),
                Err(resp) => resp,
            }
        }
        "ListAccountAssignmentsForPrincipal" => {
            // moto's four-term filter (models.py): a Filter.AccountId must
            // match the assignment's target_id (its default is the target
            // id itself, so an absent filter matches everything), plus
            // principal_id, principal_type and instance_arn.
            let filter_account_id = p
                .get("Filter")
                .and_then(|f| f.get("AccountId"))
                .filter(|v| !v.is_null())
                .cloned();
            let instance_arn = param(p, "InstanceArn");
            let principal_id = param(p, "PrincipalId");
            let principal_type = param(p, "PrincipalType");
            let items: Vec<(String, Value)> = st
                .assignments
                .iter()
                .filter(|a| {
                    filter_account_id
                        .clone()
                        .unwrap_or_else(|| a.target_id.clone())
                        == a.target_id
                        && principal_id == a.principal_id
                        && principal_type == a.principal_type
                        && instance_arn.as_str().unwrap_or_default() == a.instance_arn
                })
                .map(|a| {
                    let item = json!({
                        "AccountId": a.target_id,
                        "PermissionSetArn": a.permission_set_arn,
                        "PrincipalId": a.principal_id,
                        "PrincipalType": a.principal_type,
                    });
                    (
                        paginated_key(
                            &item,
                            &[
                                "AccountId",
                                "PermissionSetArn",
                                "PrincipalId",
                                "PrincipalType",
                            ],
                        ),
                        item,
                    )
                })
                .collect();
            match paginate_keyed(items, max_results(p), next_token(p)) {
                Ok((page, next)) => ok(&json!({"AccountAssignments": page, "NextToken": next})),
                Err(resp) => resp,
            }
        }
        "ListTagsForResource" => {
            let arn = body_str(p, "ResourceARN");
            let tags = st.tags.get(arn).cloned().unwrap_or(json!({}));
            let list: Vec<Value> = tags
                .as_object()
                .map(|m| {
                    m.iter()
                        .map(|(k, v)| json!({"Key": k, "Value": v}))
                        .collect()
                })
                .unwrap_or_default();
            ok(&json!({"Tags": list}))
        }
        "TagResource" => {
            let arn = body_str(p, "ResourceARN");
            if let Some(tags) = p.get("Tags") {
                store_tags(st, arn, tags);
            }
            ok(&json!({}))
        }
        "UntagResource" => {
            let arn = body_str(p, "ResourceARN");
            if let Some(keys) = p.get("TagKeys").and_then(Value::as_array) {
                if let Some(entry) = st.tags.get_mut(arn) {
                    for k in keys {
                        entry
                            .as_object_mut()
                            .unwrap()
                            .remove(k.as_str().unwrap_or(""));
                    }
                }
            }
            ok(&json!({}))
        }
        "ProvisionPermissionSet" => {
            let instance_arn = body_str(p, "InstanceArn");
            let ps = body_str(p, "PermissionSetArn");
            match find_ps(st, instance_arn, ps) {
                Some(i) => {
                    let arn = st.permission_sets[i].body["PermissionSetArn"]
                        .as_str()
                        .unwrap_or("")
                        .to_string();
                    // moto's handler builds the status itself: TargetType and
                    // TargetId are ignored — the permission set is provisioned
                    // to the caller's account. The built body has no
                    // FailureReason member. The record is kept so the botocore
                    // status lookups resolve it afterwards.
                    let status = json!({
                        "AccountId": req.account.to_string(),
                        "CreatedDate": epoch(),
                        "PermissionSetArn": ps,
                        "RequestId": uuid::Uuid::new_v4().to_string(),
                        "Status": "SUCCEEDED",
                    });
                    st.provisioning_statuses.push(ProvisioningStatus {
                        request_id: status["RequestId"].as_str().unwrap_or("").to_string(),
                        instance_arn: instance_arn.to_string(),
                        body: status.clone(),
                    });
                    match st.provisioned.iter_mut().find(|(i, _)| i == instance_arn) {
                        Some((_, list)) => list.push(arn),
                        None => st.provisioned.push((instance_arn.to_string(), vec![arn])),
                    }
                    ok(&json!({"PermissionSetProvisioningStatus": status}))
                }
                None => ps_not_found(ps),
            }
        }
        "ListAccountsForProvisionedPermissionSet" => {
            let instance_arn = body_str(p, "InstanceArn");
            let ps = body_str(p, "PermissionSetArn");
            let provisioned =
                provisioned_for(st, instance_arn).is_some_and(|list| list.iter().any(|a| a == ps));
            let ids = if provisioned {
                vec![req.account.to_string()]
            } else {
                Vec::new()
            };
            ok(&json!({"AccountIds": ids}))
        }
        "CreateInstance" => {
            let arn = format!("arn:aws:sso:::instance/ssoins-{}", random_string(16));
            if let Some(tags) = p.get("Tags") {
                store_tags(st, &arn, tags);
            }
            st.instances.push(json!({
                "CreatedDate": epoch(),
                "IdentityStoreId": format!("d-{}", random_string(10)),
                "InstanceArn": &arn,
                "Name": param(p, "Name"),
                "OwnerAccountId": req.account.to_string(),
                "Status": "ACTIVE",
            }));
            ok(&json!({"InstanceArn": arn}))
        }
        "DescribeInstance" => {
            let arn = body_str(p, "InstanceArn");
            match find_instance(st, arn) {
                Some(i) => {
                    let mut body = st.instances[i].clone();
                    body["EncryptionConfigurationDetails"] = json!({
                        "EncryptionStatus": "ENABLED",
                        "KeyType": "AWS_OWNED_KMS_KEY",
                    });
                    ok(&body)
                }
                None => instance_not_found(arn),
            }
        }
        "DeleteInstance" => {
            let arn = body_str(p, "InstanceArn");
            match find_instance(st, arn) {
                Some(_) => {
                    drop_instance_state(st, arn);
                    ok(&json!({}))
                }
                None => instance_not_found(arn),
            }
        }
        "AddRegion" => {
            let instance_arn = body_str(p, "InstanceArn");
            if find_instance(st, instance_arn).is_none() {
                return instance_not_found(instance_arn);
            }
            let name = body_str(p, "RegionName").to_string();
            ensure_primary_region(st, &req.region);
            if st.regions.iter().any(|r| r["RegionName"] == json!(name)) {
                return conflict(&format!("The region {name} is already on the instance"));
            }
            st.regions.push(json!({
                "RegionName": name,
                "Status": "ADDING",
                "AddedDate": epoch(),
                "IsPrimaryRegion": false,
            }));
            ok(&json!({"Status": "ADDING"}))
        }
        "DescribeRegion" => {
            let instance_arn = body_str(p, "InstanceArn");
            if find_instance(st, instance_arn).is_none() {
                return instance_not_found(instance_arn);
            }
            let name = body_str(p, "RegionName");
            ensure_primary_region(st, &req.region);
            match st.regions.iter().find(|r| r["RegionName"] == json!(name)) {
                Some(r) => ok(r),
                None => rnf(&format!("Could not find Region {name}")),
            }
        }
        "ListRegions" => {
            let instance_arn = body_str(p, "InstanceArn");
            if find_instance(st, instance_arn).is_none() {
                return instance_not_found(instance_arn);
            }
            ensure_primary_region(st, &req.region);
            ok(&json!({"Regions": st.regions, "NextToken": Value::Null}))
        }
        "RemoveRegion" => {
            let instance_arn = body_str(p, "InstanceArn");
            if find_instance(st, instance_arn).is_none() {
                return instance_not_found(instance_arn);
            }
            let name = body_str(p, "RegionName");
            let region = match st
                .regions
                .iter_mut()
                .find(|r| r["RegionName"] == json!(name))
            {
                Some(r) => r,
                None => return rnf(&format!("Could not find Region {name}")),
            };
            if region["IsPrimaryRegion"] == json!(true) {
                return conflict("The primary region cannot be removed");
            }
            // The asynchronous workflow flips ADDING/ACTIVE to REMOVING; the
            // record stays listed until the delete lands, which the twin
            // keeps pending forever.
            region["Status"] = json!("REMOVING");
            ok(&json!({"Status": "REMOVING"}))
        }
        "CreateApplication" => {
            let instance_arn = body_str(p, "InstanceArn").to_string();
            if find_instance(st, &instance_arn).is_none() {
                return instance_not_found(&instance_arn);
            }
            let arn = format!("{instance_arn}/application/apl-{}", random_string(16));
            if let Some(tags) = p.get("Tags") {
                store_tags(st, &arn, tags);
            }
            st.applications.push(AppRecord {
                instance_arn,
                body: json!({
                    "ApplicationAccount": req.account.to_string(),
                    "ApplicationArn": &arn,
                    "ApplicationProviderArn": param(p, "ApplicationProviderArn"),
                    "CreatedDate": epoch(),
                    "CreatedFrom": req.region,
                    "Description": param(p, "Description"),
                    "InstanceArn": param(p, "InstanceArn"),
                    "Name": param(p, "Name"),
                    "PortalOptions": param(p, "PortalOptions"),
                    "Status": p.get("Status").and_then(Value::as_str)
                        .map(str::to_string).unwrap_or_else(|| "ENABLED".into()),
                }),
                assignment_required: true,
                access_scopes: Vec::new(),
                auth_methods: Vec::new(),
                grants: Vec::new(),
                background_status: json!("DISABLED"),
            });
            ok(&json!({"ApplicationArn": arn}))
        }
        "DescribeApplication" => {
            let arn = body_str(p, "ApplicationArn");
            match find_application(st, arn) {
                Some(i) => ok(&st.applications[i].body),
                None => application_not_found(arn),
            }
        }
        "ListApplications" => {
            let instance_arn = body_str(p, "InstanceArn");
            if find_instance(st, instance_arn).is_none() {
                return instance_not_found(instance_arn);
            }
            let account_filter = p
                .get("Filter")
                .and_then(|f| f.get("ApplicationAccount"))
                .cloned();
            let provider_filter = p
                .get("Filter")
                .and_then(|f| f.get("ApplicationProvider"))
                .cloned();
            let list: Vec<Value> = st
                .applications
                .iter()
                .filter(|a| a.instance_arn == instance_arn)
                .filter(|a| {
                    account_filter
                        .as_ref()
                        .is_none_or(|f| *f == a.body["ApplicationAccount"])
                })
                .filter(|a| {
                    provider_filter
                        .as_ref()
                        .is_none_or(|f| *f == a.body["ApplicationProviderArn"])
                })
                .map(|a| a.body.clone())
                .collect();
            ok(&json!({"Applications": list, "NextToken": Value::Null}))
        }
        "UpdateApplication" => {
            let arn = body_str(p, "ApplicationArn");
            match find_application(st, arn) {
                Some(i) => {
                    let rec = &mut st.applications[i];
                    put_present(
                        &mut rec.body,
                        p,
                        &["Name", "Description", "PortalOptions", "Status"],
                    );
                    ok(&json!({}))
                }
                None => application_not_found(arn),
            }
        }
        "DeleteApplication" => {
            let arn = body_str(p, "ApplicationArn").to_string();
            match find_application(st, &arn) {
                Some(i) => {
                    st.applications.remove(i);
                    st.app_assignments.retain(|x| x.application_arn != arn);
                    ok(&json!({}))
                }
                None => application_not_found(&arn),
            }
        }
        "CreateApplicationAssignment" => {
            let arn = body_str(p, "ApplicationArn");
            if find_application(st, arn).is_none() {
                return application_not_found(arn);
            }
            st.app_assignments.push(AppAssignment {
                application_arn: arn.to_string(),
                principal_id: param(p, "PrincipalId"),
                principal_type: param(p, "PrincipalType"),
            });
            ok(&json!({}))
        }
        "DeleteApplicationAssignment" => {
            let arn = body_str(p, "ApplicationArn");
            let principal_id = param(p, "PrincipalId");
            let principal_type = param(p, "PrincipalType");
            match st.app_assignments.iter().position(|x| {
                x.application_arn == arn
                    && x.principal_id == principal_id
                    && x.principal_type == principal_type
            }) {
                Some(i) => {
                    st.app_assignments.remove(i);
                    ok(&json!({}))
                }
                None => rnf(&format!(
                    "Could not find ApplicationAssignment for principal {}",
                    principal_id
                )),
            }
        }
        "DescribeApplicationAssignment" => {
            let arn = body_str(p, "ApplicationArn");
            let principal_id = param(p, "PrincipalId");
            let principal_type = param(p, "PrincipalType");
            match st.app_assignments.iter().find(|x| {
                x.application_arn == arn
                    && x.principal_id == principal_id
                    && x.principal_type == principal_type
            }) {
                Some(x) => ok(&app_assignment_json(x)),
                None => rnf(&format!(
                    "Could not find ApplicationAssignment for principal {}",
                    principal_id
                )),
            }
        }
        "ListApplicationAssignments" => {
            let arn = body_str(p, "ApplicationArn");
            if find_application(st, arn).is_none() {
                return application_not_found(arn);
            }
            let list: Vec<Value> = st
                .app_assignments
                .iter()
                .filter(|x| x.application_arn == arn)
                .map(app_assignment_json)
                .collect();
            ok(&json!({"ApplicationAssignments": list, "NextToken": Value::Null}))
        }
        "ListApplicationAssignmentsForPrincipal" => {
            let instance_arn = body_str(p, "InstanceArn");
            if find_instance(st, instance_arn).is_none() {
                return instance_not_found(instance_arn);
            }
            let principal_id = param(p, "PrincipalId");
            let principal_type = param(p, "PrincipalType");
            let app_filter = p
                .get("Filter")
                .and_then(|f| f.get("ApplicationArn"))
                .cloned();
            let list: Vec<Value> = st
                .app_assignments
                .iter()
                .filter(|x| x.principal_id == principal_id && x.principal_type == principal_type)
                .filter(|x| {
                    app_filter
                        .as_ref()
                        .is_none_or(|f| *f == json!(x.application_arn))
                })
                .filter(|x| {
                    st.applications
                        .iter()
                        .find(|a| a.body["ApplicationArn"] == json!(x.application_arn))
                        .map(|a| a.instance_arn == instance_arn)
                        .unwrap_or(false)
                })
                .map(app_assignment_json)
                .collect();
            ok(&json!({"ApplicationAssignments": list, "NextToken": Value::Null}))
        }
        "GetApplicationAssignmentConfiguration" => {
            let arn = body_str(p, "ApplicationArn");
            match find_application(st, arn) {
                Some(i) => ok(&json!({
                    "AssignmentRequired": st.applications[i].assignment_required
                })),
                None => application_not_found(arn),
            }
        }
        "PutApplicationAssignmentConfiguration" => {
            let arn = body_str(p, "ApplicationArn");
            match find_application(st, arn) {
                Some(i) => {
                    st.applications[i].assignment_required = p
                        .get("AssignmentRequired")
                        .and_then(Value::as_bool)
                        .unwrap_or(true);
                    ok(&json!({}))
                }
                None => application_not_found(arn),
            }
        }
        "PutApplicationAccessScope" => {
            let arn = body_str(p, "ApplicationArn");
            match find_application(st, arn) {
                Some(i) => {
                    let rec = &mut st.applications[i];
                    let scope = body_str(p, "Scope").to_string();
                    let targets = p
                        .get("AuthorizedTargets")
                        .and_then(Value::as_array)
                        .map(|t| json!(t))
                        .unwrap_or_else(|| json!([]));
                    match rec.access_scopes.iter_mut().find(|(s, _)| *s == scope) {
                        Some((_, stored)) => *stored = targets,
                        None => rec.access_scopes.push((scope, targets)),
                    }
                    ok(&json!({}))
                }
                None => application_not_found(arn),
            }
        }
        "GetApplicationAccessScope" => {
            let arn = body_str(p, "ApplicationArn");
            match find_application(st, arn) {
                Some(i) => {
                    let scope = body_str(p, "Scope");
                    match st.applications[i]
                        .access_scopes
                        .iter()
                        .find(|(s, _)| s == scope)
                    {
                        Some((s, targets)) => {
                            ok(&json!({"Scope": s, "AuthorizedTargets": targets}))
                        }
                        None => rnf(&format!("Could not find AccessScope with name {scope}")),
                    }
                }
                None => application_not_found(arn),
            }
        }
        "ListApplicationAccessScopes" => {
            let arn = body_str(p, "ApplicationArn");
            match find_application(st, arn) {
                Some(i) => {
                    let scopes: Vec<Value> = st.applications[i]
                        .access_scopes
                        .iter()
                        .map(|(s, t)| json!({"Scope": s, "AuthorizedTargets": t}))
                        .collect();
                    ok(&json!({"Scopes": scopes, "NextToken": Value::Null}))
                }
                None => application_not_found(arn),
            }
        }
        "DeleteApplicationAccessScope" => {
            let arn = body_str(p, "ApplicationArn");
            match find_application(st, arn) {
                Some(i) => {
                    let scope = body_str(p, "Scope").to_string();
                    let rec = &mut st.applications[i];
                    match rec.access_scopes.iter().position(|(s, _)| *s == scope) {
                        Some(j) => {
                            rec.access_scopes.remove(j);
                            ok(&json!({}))
                        }
                        None => rnf(&format!("Could not find AccessScope with name {scope}")),
                    }
                }
                None => application_not_found(arn),
            }
        }
        "PutApplicationAuthenticationMethod" => {
            let arn = body_str(p, "ApplicationArn");
            match find_application(st, arn) {
                Some(i) => {
                    let rec = &mut st.applications[i];
                    let mtype = body_str(p, "AuthenticationMethodType").to_string();
                    let method = param(p, "AuthenticationMethod");
                    match rec.auth_methods.iter_mut().find(|(t, _)| *t == mtype) {
                        Some((_, stored)) => *stored = method,
                        None => rec.auth_methods.push((mtype, method)),
                    }
                    ok(&json!({}))
                }
                None => application_not_found(arn),
            }
        }
        "ListApplicationAuthenticationMethods" => {
            let arn = body_str(p, "ApplicationArn");
            match find_application(st, arn) {
                Some(i) => {
                    let list: Vec<Value> = st.applications[i]
                        .auth_methods
                        .iter()
                        .map(|(t, m)| json!({"AuthenticationMethodType": t, "AuthenticationMethod": m}))
                        .collect();
                    ok(&json!({"AuthenticationMethods": list, "NextToken": Value::Null}))
                }
                None => application_not_found(arn),
            }
        }
        "GetApplicationAuthenticationMethod" => {
            let arn = body_str(p, "ApplicationArn");
            match find_application(st, arn) {
                Some(i) => {
                    let mtype = body_str(p, "AuthenticationMethodType");
                    match st.applications[i]
                        .auth_methods
                        .iter()
                        .find(|(t, _)| t == mtype)
                    {
                        Some((_, m)) => ok(&json!({"AuthenticationMethod": m})),
                        None => rnf(&format!(
                            "Could not find AuthenticationMethod with type {mtype}"
                        )),
                    }
                }
                None => application_not_found(arn),
            }
        }
        "DeleteApplicationAuthenticationMethod" => {
            let arn = body_str(p, "ApplicationArn");
            match find_application(st, arn) {
                Some(i) => {
                    let mtype = body_str(p, "AuthenticationMethodType").to_string();
                    let rec = &mut st.applications[i];
                    match rec.auth_methods.iter().position(|(t, _)| *t == mtype) {
                        Some(j) => {
                            rec.auth_methods.remove(j);
                            ok(&json!({}))
                        }
                        None => rnf(&format!(
                            "Could not find AuthenticationMethod with type {mtype}"
                        )),
                    }
                }
                None => application_not_found(arn),
            }
        }
        "ListApplicationGrants" => {
            let arn = body_str(p, "ApplicationArn");
            match find_application(st, arn) {
                Some(i) => {
                    let list: Vec<Value> = st.applications[i]
                        .grants
                        .iter()
                        .map(|(t, g)| json!({"GrantType": t, "Grant": g}))
                        .collect();
                    ok(&json!({"Grants": list, "NextToken": Value::Null}))
                }
                None => application_not_found(arn),
            }
        }
        "GetApplicationGrant" => {
            let arn = body_str(p, "ApplicationArn");
            match find_application(st, arn) {
                Some(i) => {
                    let gtype = body_str(p, "GrantType");
                    match st.applications[i].grants.iter().find(|(t, _)| t == gtype) {
                        Some((_, g)) => ok(&json!({"Grant": g})),
                        None => rnf(&format!("Could not find Grant with type {gtype}")),
                    }
                }
                None => application_not_found(arn),
            }
        }
        "DeleteApplicationGrant" => {
            let arn = body_str(p, "ApplicationArn");
            match find_application(st, arn) {
                Some(i) => {
                    let gtype = body_str(p, "GrantType").to_string();
                    let rec = &mut st.applications[i];
                    match rec.grants.iter().position(|(t, _)| *t == gtype) {
                        Some(j) => {
                            rec.grants.remove(j);
                            ok(&json!({}))
                        }
                        None => rnf(&format!("Could not find Grant with type {gtype}")),
                    }
                }
                None => application_not_found(arn),
            }
        }
        "PutApplicationSessionConfiguration" => {
            let arn = body_str(p, "ApplicationArn");
            match find_application(st, arn) {
                Some(i) => {
                    if let Some(v) = p.get("UserBackgroundSessionApplicationStatus") {
                        st.applications[i].background_status = v.clone();
                    }
                    ok(&json!({}))
                }
                None => application_not_found(arn),
            }
        }
        "GetApplicationSessionConfiguration" => {
            let arn = body_str(p, "ApplicationArn");
            match find_application(st, arn) {
                Some(i) => ok(&json!({
                    "UserBackgroundSessionApplicationStatus":
                        st.applications[i].background_status
                })),
                None => application_not_found(arn),
            }
        }
        "CreateInstanceAccessControlAttributeConfiguration" => {
            let instance_arn = body_str(p, "InstanceArn").to_string();
            if find_instance(st, &instance_arn).is_none() {
                return instance_not_found(&instance_arn);
            }
            if st.instance_abac.iter().any(|(a, _)| a == &instance_arn) {
                return conflict(&format!(
                    "InstanceAccessControlAttributeConfiguration already exists for instance {instance_arn}"
                ));
            }
            st.instance_abac.push((
                instance_arn,
                param(p, "InstanceAccessControlAttributeConfiguration"),
            ));
            ok(&json!({}))
        }
        "DescribeInstanceAccessControlAttributeConfiguration" => {
            let instance_arn = body_str(p, "InstanceArn");
            match find_instance(st, instance_arn) {
                Some(_) => match st.instance_abac.iter().find(|(a, _)| a == instance_arn) {
                    Some((_, config)) => ok(&json!({
                        "InstanceAccessControlAttributeConfiguration": config,
                        "Status": "ENABLED",
                    })),
                    None => rnf(&format!(
                        "Could not find InstanceAccessControlAttributeConfiguration for instance {}",
                        arn_tail(instance_arn)
                    )),
                },
                None => instance_not_found(instance_arn),
            }
        }
        "UpdateInstanceAccessControlAttributeConfiguration" => {
            let instance_arn = body_str(p, "InstanceArn");
            match find_instance(st, instance_arn) {
                Some(_) => match st.instance_abac.iter_mut().find(|(a, _)| a == instance_arn) {
                    Some((_, config)) => {
                        *config = param(p, "InstanceAccessControlAttributeConfiguration");
                        ok(&json!({}))
                    }
                    None => rnf(&format!(
                        "Could not find InstanceAccessControlAttributeConfiguration for instance {}",
                        arn_tail(instance_arn)
                    )),
                },
                None => instance_not_found(instance_arn),
            }
        }
        "DeleteInstanceAccessControlAttributeConfiguration" => {
            let instance_arn = body_str(p, "InstanceArn");
            match find_instance(st, instance_arn) {
                Some(_) => match st.instance_abac.iter().position(|(a, _)| a == instance_arn) {
                    Some(i) => {
                        st.instance_abac.remove(i);
                        ok(&json!({}))
                    }
                    None => rnf(&format!(
                        "Could not find InstanceAccessControlAttributeConfiguration for instance {}",
                        arn_tail(instance_arn)
                    )),
                },
                None => instance_not_found(instance_arn),
            }
        }
        "PutPermissionsBoundaryToPermissionSet" => {
            let instance_arn = body_str(p, "InstanceArn");
            let ps = body_str(p, "PermissionSetArn");
            match find_ps(st, instance_arn, ps) {
                Some(i) => {
                    st.permission_sets[i].permissions_boundary = param(p, "PermissionsBoundary");
                    ok(&json!({}))
                }
                None => ps_not_found(ps),
            }
        }
        "GetPermissionsBoundaryForPermissionSet" => {
            let instance_arn = body_str(p, "InstanceArn");
            let ps = body_str(p, "PermissionSetArn");
            match find_ps(st, instance_arn, ps) {
                Some(i) => match st.permission_sets[i].permissions_boundary.as_object() {
                    Some(_) => ok(&json!({
                        "PermissionsBoundary": st.permission_sets[i].permissions_boundary
                    })),
                    None => rnf(&format!(
                        "Could not find PermissionsBoundary for id {}",
                        ps_id(ps)
                    )),
                },
                None => ps_not_found(ps),
            }
        }
        "DeletePermissionsBoundaryFromPermissionSet" => {
            let instance_arn = body_str(p, "InstanceArn");
            let ps = body_str(p, "PermissionSetArn");
            match find_ps(st, instance_arn, ps) {
                Some(i) => {
                    if !st.permission_sets[i].permissions_boundary.is_object() {
                        return rnf(&format!(
                            "Could not find PermissionsBoundary for id {}",
                            ps_id(ps)
                        ));
                    }
                    st.permission_sets[i].permissions_boundary = Value::Null;
                    ok(&json!({}))
                }
                None => ps_not_found(ps),
            }
        }
        "CreateTrustedTokenIssuer" => {
            let instance_arn = body_str(p, "InstanceArn").to_string();
            if find_instance(st, &instance_arn).is_none() {
                return instance_not_found(&instance_arn);
            }
            let arn = format!(
                "{instance_arn}/trustedTokenIssuer/tti-{}",
                random_string(16)
            );
            if let Some(tags) = p.get("Tags") {
                store_tags(st, &arn, tags);
            }
            st.token_issuers.push(TtiRecord {
                arn: arn.clone(),
                instance_arn,
                body: json!({
                    "Name": param(p, "Name"),
                    "TrustedTokenIssuerArn": &arn,
                    "TrustedTokenIssuerConfiguration": param(p, "TrustedTokenIssuerConfiguration"),
                    "TrustedTokenIssuerType": param(p, "TrustedTokenIssuerType"),
                }),
            });
            ok(&json!({"TrustedTokenIssuerArn": arn}))
        }
        "DescribeTrustedTokenIssuer" => {
            let arn = body_str(p, "TrustedTokenIssuerArn");
            match st.token_issuers.iter().find(|t| t.arn == arn) {
                Some(t) => ok(&t.body),
                None => rnf(&format!(
                    "Could not find TrustedTokenIssuer with id {}",
                    arn_tail(arn)
                )),
            }
        }
        "UpdateTrustedTokenIssuer" => {
            let arn = body_str(p, "TrustedTokenIssuerArn");
            match st.token_issuers.iter_mut().find(|t| t.arn == arn) {
                Some(t) => {
                    put_present(&mut t.body, p, &["Name", "TrustedTokenIssuerConfiguration"]);
                    ok(&json!({}))
                }
                None => rnf(&format!(
                    "Could not find TrustedTokenIssuer with id {}",
                    arn_tail(arn)
                )),
            }
        }
        "DeleteTrustedTokenIssuer" => {
            let arn = body_str(p, "TrustedTokenIssuerArn");
            match st.token_issuers.iter().position(|t| t.arn == arn) {
                Some(i) => {
                    st.token_issuers.remove(i);
                    ok(&json!({}))
                }
                None => rnf(&format!(
                    "Could not find TrustedTokenIssuer with id {}",
                    arn_tail(arn)
                )),
            }
        }
        "ListTrustedTokenIssuers" => {
            let instance_arn = body_str(p, "InstanceArn");
            if find_instance(st, instance_arn).is_none() {
                return instance_not_found(instance_arn);
            }
            let list: Vec<Value> = st
                .token_issuers
                .iter()
                .filter(|t| t.instance_arn == instance_arn)
                .map(|t| {
                    json!({
                        "Name": t.body["Name"],
                        "TrustedTokenIssuerArn": t.arn,
                        "TrustedTokenIssuerType": t.body["TrustedTokenIssuerType"],
                    })
                })
                .collect();
            ok(&json!({"TrustedTokenIssuers": list, "NextToken": Value::Null}))
        }
        "DescribePermissionSetProvisioningStatus" => {
            let request_id = body_str(p, "ProvisionPermissionSetRequestId");
            match st
                .provisioning_statuses
                .iter()
                .find(|s| s.request_id == request_id)
            {
                Some(s) => ok(&json!({"PermissionSetProvisioningStatus": s.body})),
                // The sibling DeleteAccountAssignment lookups answer moto's
                // empty-message RNF; keep that content for these too.
                None => rnf(""),
            }
        }
        "ListPermissionSetProvisioningStatus" => {
            let instance_arn = body_str(p, "InstanceArn");
            if find_instance(st, instance_arn).is_none() {
                return instance_not_found(instance_arn);
            }
            let keep = status_filter(p);
            let list: Vec<Value> = st
                .provisioning_statuses
                .iter()
                .filter(|s| s.instance_arn == instance_arn && keep())
                .map(|s| {
                    json!({
                        "CreatedDate": s.body["CreatedDate"],
                        "RequestId": s.body["RequestId"],
                        "Status": s.body["Status"],
                    })
                })
                .collect();
            ok(&json!({
                "PermissionSetsProvisioningStatus": list,
                "NextToken": Value::Null,
            }))
        }
        "ListAccountAssignmentCreationStatus" => {
            let instance_arn = body_str(p, "InstanceArn");
            if find_instance(st, instance_arn).is_none() {
                return instance_not_found(instance_arn);
            }
            let keep = status_filter(p);
            let list: Vec<Value> = st
                .assignments
                .iter()
                .filter(|a| a.instance_arn == instance_arn && keep())
                .map(|a| {
                    json!({
                        "CreatedDate": a.created_date,
                        "RequestId": a.request_id,
                        "Status": "SUCCEEDED",
                    })
                })
                .collect();
            ok(&json!({
                "AccountAssignmentsCreationStatus": list,
                "NextToken": Value::Null,
            }))
        }
        "ListAccountAssignmentDeletionStatus" => {
            let instance_arn = body_str(p, "InstanceArn");
            if find_instance(st, instance_arn).is_none() {
                return instance_not_found(instance_arn);
            }
            let keep = status_filter(p);
            let list: Vec<Value> = st
                .deleted_assignments
                .iter()
                .filter(|a| a.instance_arn == instance_arn && keep())
                .map(|a| {
                    json!({
                        "CreatedDate": a.created_date,
                        "RequestId": a.request_id,
                        "Status": "SUCCEEDED",
                    })
                })
                .collect();
            ok(&json!({
                "AccountAssignmentsDeletionStatus": list,
                "NextToken": Value::Null,
            }))
        }
        _ => {
            let e = json!({
                "__type": "NotImplemented",
                "message": format!("The {} action has not been implemented", req.operation),
            });
            Response::json(501, serde_json::to_vec(&e).unwrap_or_default())
        }
    }
}

/// moto's tag store: {Key: Value} per resource ARN (twin extension; moto's
/// ssoadmin responses carry no tag handlers).
fn store_tags(st: &mut State, arn: &str, tags: &Value) {
    let entry = st.tags.entry(arn.to_string()).or_insert_with(|| json!({}));
    for tag in tags.as_array().unwrap_or(&vec![]) {
        let key = tag.get("Key").and_then(Value::as_str).unwrap_or("");
        if key.is_empty() {
            continue;
        }
        entry.as_object_mut().unwrap().insert(
            key.to_string(),
            tag.get("Value").cloned().unwrap_or(Value::Null),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn req(op: &str) -> Request {
        Request {
            service: "ssoadmin".into(),
            operation: op.into(),
            method: "POST".into(),
            path: "/".into(),
            query_string: String::new(),
            headers: Default::default(),
            body: bytes::Bytes::new(),
            account: 123456789012,
            region: "us-east-1".into(),
        }
    }

    /// Every test takes a fresh account so the shared store stays isolated.
    fn fresh_state(account: u64) -> State {
        let mut st = State::default();
        with_default_instance(&mut st, account);
        st
    }

    fn body(resp: &Response) -> Value {
        serde_json::from_slice(&resp.body).unwrap()
    }

    fn instance_arn(st: &State) -> String {
        st.instances[0]["InstanceArn"].as_str().unwrap().to_string()
    }

    fn create_permission_set(st: &mut State, name: &str) -> String {
        let inst = instance_arn(st);
        let resp = handle(
            st,
            &json!({"Name": name, "InstanceArn": &inst}),
            &req("CreatePermissionSet"),
        );
        body(&resp)["PermissionSet"]["PermissionSetArn"]
            .as_str()
            .unwrap()
            .to_string()
    }

    /// The dataset (name -> path) parsed from the embedded JSON.
    fn dataset() -> &'static HashMap<String, String> {
        static POLICIES: Lazy<HashMap<String, String>> = Lazy::new(|| {
            serde_json::from_str(include_str!("ssoadmin_data/aws_managed_policies.json"))
                .unwrap_or_default()
        });
        &POLICIES
    }

    /// The exact Amazon-managed-policy ARN for `name` (dataset paths vary).
    fn policy_arn(name: &str) -> String {
        match dataset().get(name) {
            Some(path) => format!("arn:aws:iam::aws:policy{path}{name}"),
            None => format!("arn:aws:iam::aws:policy/{name}"),
        }
    }

    /// A hand-built resume token: base64 of `{"uniqueAttributes": attrs}`.
    fn token_for(attrs: &str) -> String {
        base64::engine::general_purpose::STANDARD
            .encode(serde_json::to_vec(&json!({ "uniqueAttributes": attrs })).unwrap())
    }

    fn attach_managed(st: &mut State, inst: &str, ps: &str, policy: &str) -> Response {
        handle(
            st,
            &json!({
                "InstanceArn": inst,
                "PermissionSetArn": ps,
                "ManagedPolicyArn": policy
            }),
            &req("AttachManagedPolicyToPermissionSet"),
        )
    }

    fn list_managed(st: &mut State, inst: &str, ps: &str) -> Value {
        body(&handle(
            st,
            &json!({"InstanceArn": inst, "PermissionSetArn": ps}),
            &req("ListManagedPoliciesInPermissionSet"),
        ))
    }

    fn attach_customer(
        st: &mut State,
        inst: &str,
        ps: &str,
        name: &str,
        path: Option<&str>,
    ) -> Response {
        let mut reference = json!({"Name": name});
        if let Some(p) = path {
            reference["Path"] = json!(p);
        }
        handle(
            st,
            &json!({
                "InstanceArn": inst,
                "PermissionSetArn": ps,
                "CustomerManagedPolicyReference": reference
            }),
            &req("AttachCustomerManagedPolicyReferenceToPermissionSet"),
        )
    }

    #[test]
    fn instance_and_permission_sets() {
        let st = &mut fresh_state(111_000_000_001);
        let listed = body(&handle(st, &json!({}), &req("ListInstances")));
        assert_eq!(listed["Instances"][0]["Status"], "ACTIVE");
        let arn = instance_arn(st);

        let created = body(&handle(
            st,
            &json!({"Name": "ps", "InstanceArn": &arn, "Description": "d"}),
            &req("CreatePermissionSet"),
        ));
        let ps = created["PermissionSet"]["PermissionSetArn"]
            .as_str()
            .unwrap()
            .to_string();
        assert!(ps.starts_with(&arn));

        let listed = body(&handle(
            st,
            &json!({"InstanceArn": &arn}),
            &req("ListPermissionSets"),
        ));
        assert_eq!(listed["PermissionSets"].as_array().unwrap().len(), 1);

        let assignment = body(&handle(
            st,
            &json!({
                "InstanceArn": &arn,
                "PermissionSetArn": &ps,
                "PrincipalId": "user-1",
                "PrincipalType": "USER",
                "TargetId": "123456789012",
                "TargetType": "AWS_ACCOUNT",
            }),
            &req("CreateAccountAssignment"),
        ));
        assert_eq!(
            assignment["AccountAssignmentCreationStatus"]["Status"],
            "SUCCEEDED"
        );

        handle(
            st,
            &json!({"InstanceArn": &arn, "PermissionSetArn": &ps}),
            &req("DeletePermissionSet"),
        );
        assert_eq!(st.permission_sets.len(), 0);
    }

    #[test]
    fn describe_and_update_permission_set() {
        let st = &mut fresh_state(111_000_000_002);
        let ps = create_permission_set(st, "ps");
        let inst = instance_arn(st);

        let described = body(&handle(
            st,
            &json!({"InstanceArn": &inst, "PermissionSetArn": &ps}),
            &req("DescribePermissionSet"),
        ));
        assert_eq!(described["PermissionSet"]["Name"], "ps");
        assert!(described["PermissionSet"]["CreatedDate"].is_number());

        // moto overwrites unconditionally on update: an omitted description
        // reads null and SessionDuration resets to 3600.
        handle(
            st,
            &json!({"InstanceArn": &inst, "PermissionSetArn": &ps, "SessionDuration": 900}),
            &req("UpdatePermissionSet"),
        );
        let described = body(&handle(
            st,
            &json!({"InstanceArn": &inst, "PermissionSetArn": &ps}),
            &req("DescribePermissionSet"),
        ));
        assert_eq!(described["PermissionSet"]["Description"], Value::Null);
        assert_eq!(described["PermissionSet"]["SessionDuration"], 900);
        assert_eq!(described["PermissionSet"]["RelayState"], Value::Null);

        // Unknown permission sets raise moto's message with the ARN tail.
        let missing = handle(
            st,
            &json!({"InstanceArn": &inst, "PermissionSetArn": format!("{inst}/ps-unknown")}),
            &req("DescribePermissionSet"),
        );
        assert_eq!(missing.status, 400);
        let e = body(&missing);
        assert_eq!(e["__type"], "ResourceNotFoundException");
        assert_eq!(
            e["message"],
            "Could not find PermissionSet with id ps-unknown"
        );
    }

    #[test]
    fn inline_policy_roundtrip() {
        let st = &mut fresh_state(111_000_000_003);
        let ps = create_permission_set(st, "ps");
        let inst = instance_arn(st);

        // moto defaults the inline policy to "".
        let got = body(&handle(
            st,
            &json!({"InstanceArn": &inst, "PermissionSetArn": &ps}),
            &req("GetInlinePolicyForPermissionSet"),
        ));
        assert_eq!(got["InlinePolicy"], "");

        let put = body(&handle(
            st,
            &json!({
                "InstanceArn": &inst,
                "PermissionSetArn": &ps,
                "InlinePolicy": "{\"Version\":\"2012-10-17\"}"
            }),
            &req("PutInlinePolicyToPermissionSet"),
        ));
        assert!(put.is_object());
        let got = body(&handle(
            st,
            &json!({"InstanceArn": &inst, "PermissionSetArn": &ps}),
            &req("GetInlinePolicyForPermissionSet"),
        ));
        assert_eq!(got["InlinePolicy"], "{\"Version\":\"2012-10-17\"}");

        // delete resets to "".
        handle(
            st,
            &json!({"InstanceArn": &inst, "PermissionSetArn": &ps}),
            &req("DeleteInlinePolicyFromPermissionSet"),
        );
        let got = body(&handle(
            st,
            &json!({"InstanceArn": &inst, "PermissionSetArn": &ps}),
            &req("GetInlinePolicyForPermissionSet"),
        ));
        assert_eq!(got["InlinePolicy"], "");

        // unknown permission set → moto's message with the ARN tail.
        let missing = handle(
            st,
            &json!({"InstanceArn": &inst, "PermissionSetArn": format!("{inst}/ps-unknown")}),
            &req("GetInlinePolicyForPermissionSet"),
        );
        assert_eq!(missing.status, 400);
        let e = body(&missing);
        assert_eq!(e["__type"], "ResourceNotFoundException");
        assert_eq!(
            e["message"],
            "Could not find PermissionSet with id ps-unknown"
        );
    }

    #[test]
    fn managed_policies_roundtrip() {
        let st = &mut fresh_state(111_000_000_004);
        let ps = create_permission_set(st, "ps");
        let inst = instance_arn(st);

        // A real AWS managed policy attaches.
        let arn = "arn:aws:iam::aws:policy/AdministratorAccess";
        assert_eq!(attach_managed(st, &inst, &ps, arn).status, 200);
        let list = list_managed(st, &inst, &ps);
        let policies = list["AttachedManagedPolicies"].as_array().unwrap();
        assert_eq!(policies.len(), 1);
        assert_eq!(policies[0]["Name"], "AdministratorAccess");
        assert_eq!(policies[0]["Arn"], arn);

        // A second attach of the same policy conflicts (moto's message).
        let dup = attach_managed(st, &inst, &ps, arn);
        assert_eq!(dup.status, 400);
        let e = body(&dup);
        assert_eq!(e["__type"], "ConflictException");
        assert_eq!(
            e["message"],
            format!(
                "Permission set with id {} already has a typed link attachment to a manged policy with {arn}",
                ps_id(&ps)
            )
        );

        // Detaching an un-attached policy → moto's message.
        let missing = attach_detached(st, &inst, &ps);
        assert_eq!(missing.status, 400);
        let e = body(&missing);
        assert_eq!(e["__type"], "ResourceNotFoundException");
        assert_eq!(
            e["message"],
            "Could not find ManagedPolicy with arn arn:aws:iam::aws:policy/ViewOnlyAccess"
        );

        // Unknown policy names fail validation (moto's message).
        let bogus = attach_managed(st, &inst, &ps, "arn:aws:iam::aws:policy/NoSuchPolicy");
        assert_eq!(bogus.status, 400);
        let e = body(&bogus);
        assert_eq!(
            e["message"],
            "Policy does not exist with ARN: arn:aws:iam::aws:policy/NoSuchPolicy"
        );

        // Detach the attached policy and the list empties.
        let detached = handle(
            st,
            &json!({"InstanceArn": &inst, "PermissionSetArn": &ps, "ManagedPolicyArn": arn}),
            &req("DetachManagedPolicyFromPermissionSet"),
        );
        assert_eq!(detached.status, 200);
        let list = list_managed(st, &inst, &ps);
        assert_eq!(list["AttachedManagedPolicies"].as_array().unwrap().len(), 0);
    }

    fn attach_detached(st: &mut State, inst: &str, ps: &str) -> Response {
        handle(
            st,
            &json!({
                "InstanceArn": inst,
                "PermissionSetArn": ps,
                "ManagedPolicyArn": "arn:aws:iam::aws:policy/ViewOnlyAccess"
            }),
            &req("DetachManagedPolicyFromPermissionSet"),
        )
    }

    #[test]
    fn managed_policy_quota() {
        let st = &mut fresh_state(111_000_000_005);
        let ps = create_permission_set(st, "ps");
        let inst = instance_arn(st);

        // Names from the embedded dataset, sorted for determinism.
        let names: Vec<String> = {
            let mut names: Vec<String> = dataset().keys().cloned().collect();
            names.sort();
            names.truncate(21);
            names
        };
        for name in &names[..20] {
            let arn = policy_arn(name);
            let resp = attach_managed(st, &inst, &ps, &arn);
            assert_eq!(resp.status, 200, "attach {arn}");
        }
        // The 21st attach raises ServiceQuotaExceededException.
        let overrun = attach_managed(st, &inst, &ps, &policy_arn(&names[20]));
        assert_eq!(overrun.status, 400);
        let e = body(&overrun);
        assert_eq!(e["__type"], "ServiceQuotaExceededException");
        assert_eq!(
            e["message"],
            format!(
                "You have exceeded AWS SSO limits. Cannot create ManagedPolicy more than 20 for id {}. Please refer to https://docs.aws.amazon.com/singlesignon/latest/userguide/limits.html",
                ps_id(&ps)
            )
        );
    }

    #[test]
    fn customer_managed_policies_roundtrip() {
        let st = &mut fresh_state(111_000_000_006);
        let ps = create_permission_set(st, "ps");
        let inst = instance_arn(st);

        // attach without Path — moto defaults the path to "/".
        assert_eq!(
            attach_customer(st, &inst, &ps, "TeamPolicy", None).status,
            200
        );
        // A second reference with an explicit path is a distinct policy.
        assert_eq!(
            attach_customer(st, &inst, &ps, "TeamPolicy", Some("/team/")).status,
            200
        );
        let list = body(&handle(
            st,
            &json!({"InstanceArn": &inst, "PermissionSetArn": &ps}),
            &req("ListCustomerManagedPolicyReferencesInPermissionSet"),
        ));
        let references = list["CustomerManagedPolicyReferences"].as_array().unwrap();
        assert_eq!(references.len(), 2);
        assert_eq!(references[0]["Path"], "/");
        assert_eq!(references[1]["Path"], "/team/");

        // duplicate attach of the default-path policy conflicts.
        let dup = attach_customer(st, &inst, &ps, "TeamPolicy", None);
        assert_eq!(dup.status, 400);
        let e = body(&dup);
        assert_eq!(e["__type"], "ConflictException");
        assert_eq!(
            e["message"],
            "Given customer managed policy with name: TeamPolicy  and path / already attached"
        );

        // detach without Path hits the default path.
        let detached = handle(
            st,
            &json!({
                "InstanceArn": &inst,
                "PermissionSetArn": &ps,
                "CustomerManagedPolicyReference": {"Name": "TeamPolicy"}
            }),
            &req("DetachCustomerManagedPolicyReferenceFromPermissionSet"),
        );
        assert_eq!(detached.status, 200);

        // detaching again → moto's message.
        let missing = handle(
            st,
            &json!({
                "InstanceArn": &inst,
                "PermissionSetArn": &ps,
                "CustomerManagedPolicyReference": {"Name": "TeamPolicy"}
            }),
            &req("DetachCustomerManagedPolicyReferenceFromPermissionSet"),
        );
        assert_eq!(missing.status, 400);
        let e = body(&missing);
        assert_eq!(e["__type"], "ResourceNotFoundException");
        assert_eq!(
            e["message"],
            "Given managed policy with name: TeamPolicy  and path / does not exist on PermissionSet"
        );
    }

    #[test]
    fn account_assignment_statuses() {
        let st = &mut fresh_state(111_000_000_007);
        let ps = create_permission_set(st, "ps");
        let inst = instance_arn(st);
        let params = json!({
            "InstanceArn": &inst,
            "PermissionSetArn": &ps,
            "PrincipalId": "user-1",
            "PrincipalType": "USER",
            "TargetId": "123456789012",
            "TargetType": "AWS_ACCOUNT",
        });

        let created = body(&handle(st, &params, &req("CreateAccountAssignment")));
        let status = &created["AccountAssignmentCreationStatus"];
        assert_eq!(status["Status"], "SUCCEEDED");
        let request_id = status["RequestId"].as_str().unwrap().to_string();
        assert!(status["CreatedDate"].is_number());

        // the stored request id resolves through the Describe op.
        let described = body(&handle(
            st,
            &json!({"InstanceArn": &inst, "AccountAssignmentCreationRequestId": &request_id}),
            &req("DescribeAccountAssignmentCreationStatus"),
        ));
        assert_eq!(
            described["AccountAssignmentCreationStatus"]["Status"],
            "SUCCEEDED"
        );
        assert_eq!(
            described["AccountAssignmentCreationStatus"]["PrincipalId"],
            "user-1"
        );

        // unknown request ids → moto's empty-message ResourceNotFoundException.
        let missing = handle(
            st,
            &json!({"InstanceArn": &inst, "AccountAssignmentCreationRequestId": "nope"}),
            &req("DescribeAccountAssignmentCreationStatus"),
        );
        assert_eq!(missing.status, 400);
        let e = body(&missing);
        assert_eq!(e["__type"], "ResourceNotFoundException");
        assert_eq!(e["message"], "");

        // delete reports the deletion status and archives the request id.
        let deleted = body(&handle(st, &params, &req("DeleteAccountAssignment")));
        assert_eq!(
            deleted["AccountAssignmentDeletionStatus"]["Status"],
            "SUCCEEDED"
        );
        let del_request_id = deleted["AccountAssignmentDeletionStatus"]["RequestId"]
            .as_str()
            .unwrap()
            .to_string();
        let described = body(&handle(
            st,
            &json!({"InstanceArn": &inst, "AccountAssignmentDeletionRequestId": &del_request_id}),
            &req("DescribeAccountAssignmentDeletionStatus"),
        ));
        assert_eq!(
            described["AccountAssignmentDeletionStatus"]["Status"],
            "SUCCEEDED"
        );
        assert_eq!(
            described["AccountAssignmentDeletionStatus"]["TargetId"],
            "123456789012"
        );

        // deleting again — nothing matched → ResourceNotFoundException.
        let missing = handle(st, &params, &req("DeleteAccountAssignment"));
        assert_eq!(missing.status, 400);
        let e = body(&missing);
        assert_eq!(e["__type"], "ResourceNotFoundException");
    }

    #[test]
    fn provisioning_records() {
        let st = &mut fresh_state(111_000_000_008);
        let ps = create_permission_set(st, "ps");
        let inst = instance_arn(st);

        // moto ignores TargetId/TargetType and provisions to the caller.
        let provisioned = body(&handle(
            st,
            &json!({"InstanceArn": &inst, "PermissionSetArn": &ps, "TargetId": "ignored"}),
            &req("ProvisionPermissionSet"),
        ));
        let status = &provisioned["PermissionSetProvisioningStatus"];
        assert_eq!(status["Status"], "SUCCEEDED");
        assert_eq!(status["PermissionSetArn"], ps);
        assert_eq!(
            status["AccountId"],
            req("ListInstances").account.to_string()
        );

        let listed = body(&handle(
            st,
            &json!({"InstanceArn": &inst}),
            &req("ListPermissionSetsProvisionedToAccount"),
        ));
        assert_eq!(listed["PermissionSets"].as_array().unwrap().len(), 1);

        let accounts = body(&handle(
            st,
            &json!({"InstanceArn": &inst, "PermissionSetArn": &ps}),
            &req("ListAccountsForProvisionedPermissionSet"),
        ));
        assert_eq!(
            accounts["AccountIds"],
            json!([req("ListInstances").account.to_string()])
        );

        // an unprovisioned permission set lists no accounts.
        let ps2 = create_permission_set(st, "ps2");
        let accounts = body(&handle(
            st,
            &json!({"InstanceArn": &inst, "PermissionSetArn": &ps2}),
            &req("ListAccountsForProvisionedPermissionSet"),
        ));
        assert_eq!(accounts["AccountIds"].as_array().unwrap().len(), 0);

        // deleting removes the provisioning record.
        handle(
            st,
            &json!({"InstanceArn": &inst, "PermissionSetArn": &ps}),
            &req("DeletePermissionSet"),
        );
        let listed = body(&handle(
            st,
            &json!({"InstanceArn": &inst}),
            &req("ListPermissionSetsProvisionedToAccount"),
        ));
        assert_eq!(listed["PermissionSets"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn update_instance() {
        let st = &mut fresh_state(111_000_000_009);
        let inst = instance_arn(st);

        handle(
            st,
            &json!({"InstanceArn": &inst, "Name": "renamed"}),
            &req("UpdateInstance"),
        );
        let listed = body(&handle(st, &json!({}), &req("ListInstances")));
        assert_eq!(listed["Instances"][0]["Name"], "renamed");

        // unknown instance arn → silent no-op, no error.
        handle(
            st,
            &json!({"InstanceArn": "arn:aws:sso:::instance/ssoins-none", "Name": "x"}),
            &req("UpdateInstance"),
        );
        let listed = body(&handle(st, &json!({}), &req("ListInstances")));
        assert_eq!(listed["Instances"][0]["Name"], "renamed");
    }

    fn create_application(st: &mut State, inst: &str, name: &str) -> String {
        let resp = handle(
            st,
            &json!({
                "InstanceArn": inst,
                "ApplicationProviderArn": "arn:aws:sso:::applicationProvider/custom",
                "Name": name,
            }),
            &req("CreateApplication"),
        );
        body(&resp)["ApplicationArn"].as_str().unwrap().to_string()
    }

    #[test]
    fn instances_and_regions() {
        let st = &mut fresh_state(111_000_000_010);
        let seeded = instance_arn(st);

        let created = body(&handle(
            st,
            &json!({"Name": "second"}),
            &req("CreateInstance"),
        ));
        let arn = created["InstanceArn"].as_str().unwrap().to_string();
        assert!(arn.starts_with("arn:aws:sso:::instance/ssoins-"));
        assert!(arn != seeded);
        assert_eq!(st.instances.len(), 2);

        let described = body(&handle(
            st,
            &json!({"InstanceArn": &arn}),
            &req("DescribeInstance"),
        ));
        assert_eq!(described["Name"], "second");
        assert_eq!(described["OwnerAccountId"], "123456789012");
        assert_eq!(described["Status"], "ACTIVE");
        assert_eq!(
            described["EncryptionConfigurationDetails"]["KeyType"],
            "AWS_OWNED_KMS_KEY"
        );

        // regions seed lazily with the caller's region as primary.
        let listed = body(&handle(
            st,
            &json!({"InstanceArn": &arn}),
            &req("ListRegions"),
        ));
        assert_eq!(listed["Regions"][0]["RegionName"], "us-east-1");
        assert_eq!(listed["Regions"][0]["IsPrimaryRegion"], true);
        assert_eq!(listed["Regions"][0]["Status"], "ACTIVE");

        // a duplicate add conflicts; a new one lands ADDING.
        let added = body(&handle(
            st,
            &json!({"InstanceArn": &arn, "RegionName": "eu-west-1"}),
            &req("AddRegion"),
        ));
        assert_eq!(added["Status"], "ADDING");
        let dup = handle(
            st,
            &json!({"InstanceArn": &arn, "RegionName": "eu-west-1"}),
            &req("AddRegion"),
        );
        assert_eq!(dup.status, 400);
        assert_eq!(body(&dup)["__type"], "ConflictException");

        let described = body(&handle(
            st,
            &json!({"InstanceArn": &arn, "RegionName": "eu-west-1"}),
            &req("DescribeRegion"),
        ));
        assert_eq!(described["Status"], "ADDING");

        // the primary cannot be removed; the added one flips REMOVING.
        let primary = handle(
            st,
            &json!({"InstanceArn": &arn, "RegionName": "us-east-1"}),
            &req("RemoveRegion"),
        );
        assert_eq!(primary.status, 400);
        assert_eq!(body(&primary)["__type"], "ConflictException");
        let removed = body(&handle(
            st,
            &json!({"InstanceArn": &arn, "RegionName": "eu-west-1"}),
            &req("RemoveRegion"),
        ));
        assert_eq!(removed["Status"], "REMOVING");

        // unknown instance / region → modeled RNF.
        let missing = handle(
            st,
            &json!({"InstanceArn": "arn:aws:sso:::instance/ssoins-nope", "RegionName": "us-east-1"}),
            &req("AddRegion"),
        );
        assert_eq!(missing.status, 400);
        assert_eq!(body(&missing)["__type"], "ResourceNotFoundException");

        // delete cascades …
        handle(st, &json!({"InstanceArn": &arn}), &req("DeleteInstance"));
        assert_eq!(st.instances.len(), 1);
        let vanished = handle(st, &json!({"InstanceArn": &arn}), &req("DescribeInstance"));
        assert_eq!(vanished.status, 400);
        assert_eq!(body(&vanished)["__type"], "ResourceNotFoundException");
    }

    #[test]
    fn applications_surface() {
        let st = &mut fresh_state(111_000_000_011);
        let inst = instance_arn(st);
        let app = create_application(st, &inst, "app");

        let described = body(&handle(
            st,
            &json!({"ApplicationArn": &app}),
            &req("DescribeApplication"),
        ));
        assert_eq!(described["Name"], "app");
        assert_eq!(described["Status"], "ENABLED");
        assert_eq!(described["InstanceArn"], inst);
        assert_eq!(described["ApplicationAccount"], "123456789012");

        // updates touch present members only.
        handle(
            st,
            &json!({"ApplicationArn": &app, "Description": "d", "Status": "DISABLED"}),
            &req("UpdateApplication"),
        );
        let described = body(&handle(
            st,
            &json!({"ApplicationArn": &app}),
            &req("DescribeApplication"),
        ));
        assert_eq!(described["Description"], "d");
        assert_eq!(described["Status"], "DISABLED");
        assert_eq!(described["Name"], "app");

        let listed = body(&handle(
            st,
            &json!({"InstanceArn": &inst}),
            &req("ListApplications"),
        ));
        assert_eq!(listed["Applications"].as_array().unwrap().len(), 1);

        // assignment configuration defaults to required and flips on put.
        let cfg = body(&handle(
            st,
            &json!({"ApplicationArn": &app}),
            &req("GetApplicationAssignmentConfiguration"),
        ));
        assert_eq!(cfg["AssignmentRequired"], true);
        handle(
            st,
            &json!({"ApplicationArn": &app, "AssignmentRequired": false}),
            &req("PutApplicationAssignmentConfiguration"),
        );
        let cfg = body(&handle(
            st,
            &json!({"ApplicationArn": &app}),
            &req("GetApplicationAssignmentConfiguration"),
        ));
        assert_eq!(cfg["AssignmentRequired"], false);

        // session configuration starts DISABLED.
        let cfg = body(&handle(
            st,
            &json!({"ApplicationArn": &app}),
            &req("GetApplicationSessionConfiguration"),
        ));
        assert_eq!(cfg["UserBackgroundSessionApplicationStatus"], "DISABLED");
        handle(
            st,
            &json!({"ApplicationArn": &app, "UserBackgroundSessionApplicationStatus": "ENABLED"}),
            &req("PutApplicationSessionConfiguration"),
        );
        let cfg = body(&handle(
            st,
            &json!({"ApplicationArn": &app}),
            &req("GetApplicationSessionConfiguration"),
        ));
        assert_eq!(cfg["UserBackgroundSessionApplicationStatus"], "ENABLED");

        // assignments round-trip and list for a principal.
        handle(
            st,
            &json!({"ApplicationArn": &app, "PrincipalId": "u-1", "PrincipalType": "USER"}),
            &req("CreateApplicationAssignment"),
        );
        let describe = body(&handle(
            st,
            &json!({"ApplicationArn": &app, "PrincipalId": "u-1", "PrincipalType": "USER"}),
            &req("DescribeApplicationAssignment"),
        ));
        assert_eq!(describe["PrincipalType"], "USER");

        let listed = body(&handle(
            st,
            &json!({"ApplicationArn": &app}),
            &req("ListApplicationAssignments"),
        ));
        assert_eq!(listed["ApplicationAssignments"][0]["PrincipalId"], "u-1");

        let listed = body(&handle(
            st,
            &json!({"InstanceArn": &inst, "PrincipalId": "u-1", "PrincipalType": "USER"}),
            &req("ListApplicationAssignmentsForPrincipal"),
        ));
        assert_eq!(listed["ApplicationAssignments"][0]["ApplicationArn"], app);

        handle(
            st,
            &json!({"ApplicationArn": &app, "PrincipalId": "u-1", "PrincipalType": "USER"}),
            &req("DeleteApplicationAssignment"),
        );
        let listed = body(&handle(
            st,
            &json!({"ApplicationArn": &app}),
            &req("ListApplicationAssignments"),
        ));
        assert_eq!(
            listed["ApplicationAssignments"].as_array().unwrap().len(),
            0
        );

        // access scopes round-trip.
        handle(
            st,
            &json!({"ApplicationArn": &app, "Scope": "sso:accounts:access", "AuthorizedTargets": ["arn:target"]}),
            &req("PutApplicationAccessScope"),
        );
        let scope = body(&handle(
            st,
            &json!({"ApplicationArn": &app, "Scope": "sso:accounts:access"}),
            &req("GetApplicationAccessScope"),
        ));
        assert_eq!(scope["AuthorizedTargets"], json!(["arn:target"]));
        let listed = body(&handle(
            st,
            &json!({"ApplicationArn": &app}),
            &req("ListApplicationAccessScopes"),
        ));
        assert_eq!(listed["Scopes"][0]["Scope"], "sso:accounts:access");
        // an unknown scope → modeled RNF.
        let missing = handle(
            st,
            &json!({"ApplicationArn": &app, "Scope": "nope"}),
            &req("GetApplicationAccessScope"),
        );
        assert_eq!(missing.status, 400);
        assert_eq!(body(&missing)["__type"], "ResourceNotFoundException");

        handle(
            st,
            &json!({"ApplicationArn": &app, "Scope": "sso:accounts:access"}),
            &req("DeleteApplicationAccessScope"),
        );
        let listed = body(&handle(
            st,
            &json!({"ApplicationArn": &app}),
            &req("ListApplicationAccessScopes"),
        ));
        assert_eq!(listed["Scopes"].as_array().unwrap().len(), 0);

        // authentication methods round-trip by type.
        handle(
            st,
            &json!({
                "ApplicationArn": &app,
                "AuthenticationMethodType": "IAM",
                "AuthenticationMethod": {"Iam": {"ActorPolicy": {"Version": "2012-10-17"}}},
            }),
            &req("PutApplicationAuthenticationMethod"),
        );
        let method = body(&handle(
            st,
            &json!({"ApplicationArn": &app, "AuthenticationMethodType": "IAM"}),
            &req("GetApplicationAuthenticationMethod"),
        ));
        assert_eq!(
            method["AuthenticationMethod"]["Iam"]["ActorPolicy"]["Version"],
            "2012-10-17"
        );
        let listed = body(&handle(
            st,
            &json!({"ApplicationArn": &app}),
            &req("ListApplicationAuthenticationMethods"),
        ));
        assert_eq!(
            listed["AuthenticationMethods"][0]["AuthenticationMethodType"],
            "IAM"
        );
        handle(
            st,
            &json!({"ApplicationArn": &app, "AuthenticationMethodType": "IAM"}),
            &req("DeleteApplicationAuthenticationMethod"),
        );
        let listed = body(&handle(
            st,
            &json!({"ApplicationArn": &app}),
            &req("ListApplicationAuthenticationMethods"),
        ));
        assert_eq!(listed["AuthenticationMethods"].as_array().unwrap().len(), 0);

        // grants: nothing stored → list empty / get & delete RNF.
        let listed = body(&handle(
            st,
            &json!({"ApplicationArn": &app}),
            &req("ListApplicationGrants"),
        ));
        assert_eq!(listed["Grants"].as_array().unwrap().len(), 0);
        let missing = handle(
            st,
            &json!({"ApplicationArn": &app, "GrantType": "authorization_code"}),
            &req("GetApplicationGrant"),
        );
        assert_eq!(missing.status, 400);
        assert_eq!(body(&missing)["__type"], "ResourceNotFoundException");

        // delete cascades to the application's assignments.
        handle(
            st,
            &json!({"ApplicationArn": &app, "PrincipalId": "u-2", "PrincipalType": "GROUP"}),
            &req("CreateApplicationAssignment"),
        );
        handle(
            st,
            &json!({"ApplicationArn": &app}),
            &req("DeleteApplication"),
        );
        assert_eq!(st.applications.len(), 0);
        assert_eq!(st.app_assignments.len(), 0);
        let vanished = handle(
            st,
            &json!({"ApplicationArn": &app}),
            &req("DescribeApplication"),
        );
        assert_eq!(vanished.status, 400);
        assert_eq!(body(&vanished)["__type"], "ResourceNotFoundException");
    }

    #[test]
    fn application_not_found_envelopes() {
        let st = &mut fresh_state(111_000_000_012);
        let inst = instance_arn(st);

        // unknown instance arns → modeled RNF across the op families.
        for op in [
            "ListApplications",
            "ListRegions",
            "ListTrustedTokenIssuers",
            "ListPermissionSetProvisioningStatus",
        ] {
            let params = json!({"InstanceArn": "arn:aws:sso:::instance/ssoins-nope"});
            let resp = handle(st, &params, &req(op));
            assert_eq!(resp.status, 400, "{op}");
            assert_eq!(body(&resp)["__type"], "ResourceNotFoundException", "{op}");
        }
        let mut params = json!({"InstanceArn": "arn:aws:sso:::instance/ssoins-nope"});
        params["ApplicationProviderArn"] = json!("arn:aws:sso:::applicationProvider/x");
        params["Name"] = json!("app");
        let resp = handle(st, &params, &req("CreateApplication"));
        assert_eq!(resp.status, 400);
        assert_eq!(body(&resp)["__type"], "ResourceNotFoundException");

        // unknown application arn → modeled RNF.
        let missing = handle(
            st,
            &json!({"ApplicationArn": &format!("{inst}/application/apl-nope")}),
            &req("DescribeApplication"),
        );
        assert_eq!(missing.status, 400);
        assert_eq!(body(&missing)["__type"], "ResourceNotFoundException");
    }

    #[test]
    fn trusted_token_issuers_surface() {
        let st = &mut fresh_state(111_000_000_013);
        let inst = instance_arn(st);

        let created = body(&handle(
            st,
            &json!({
                "InstanceArn": &inst,
                "Name": "iss",
                "TrustedTokenIssuerType": "OIDC_JWT",
                "TrustedTokenIssuerConfiguration": {
                    "IssuerUrl": "https://issuer.example.com",
                    "ClaimAttributePath": "sub",
                    "IdentityStoreAttributePath": "userName",
                    "JwksRetrievalOption": "OPEN_ID_DISCOVERY",
                },
            }),
            &req("CreateTrustedTokenIssuer"),
        ));
        let arn = created["TrustedTokenIssuerArn"]
            .as_str()
            .unwrap()
            .to_string();
        assert!(arn.starts_with(&inst));

        let described = body(&handle(
            st,
            &json!({"TrustedTokenIssuerArn": &arn}),
            &req("DescribeTrustedTokenIssuer"),
        ));
        assert_eq!(described["Name"], "iss");
        assert_eq!(described["TrustedTokenIssuerType"], "OIDC_JWT");
        assert_eq!(
            described["TrustedTokenIssuerConfiguration"]["IssuerUrl"],
            "https://issuer.example.com"
        );

        let listed = body(&handle(
            st,
            &json!({"InstanceArn": &inst}),
            &req("ListTrustedTokenIssuers"),
        ));
        assert_eq!(listed["TrustedTokenIssuers"][0]["Name"], "iss");

        // updates touch present members only.
        handle(
            st,
            &json!({
                "TrustedTokenIssuerArn": &arn,
                "Name": "renamed",
                "TrustedTokenIssuerConfiguration": {
                    "ClaimAttributePath": "groups",
                },
            }),
            &req("UpdateTrustedTokenIssuer"),
        );
        let described = body(&handle(
            st,
            &json!({"TrustedTokenIssuerArn": &arn}),
            &req("DescribeTrustedTokenIssuer"),
        ));
        assert_eq!(described["Name"], "renamed");
        assert_eq!(
            described["TrustedTokenIssuerConfiguration"]["ClaimAttributePath"],
            "groups"
        );

        handle(
            st,
            &json!({"TrustedTokenIssuerArn": &arn}),
            &req("DeleteTrustedTokenIssuer"),
        );
        let missing = handle(
            st,
            &json!({"TrustedTokenIssuerArn": &arn}),
            &req("DescribeTrustedTokenIssuer"),
        );
        assert_eq!(missing.status, 400);
        assert_eq!(body(&missing)["__type"], "ResourceNotFoundException");
    }

    #[test]
    fn abac_and_permissions_boundaries() {
        let st = &mut fresh_state(111_000_000_014);
        let inst = instance_arn(st);
        let ps = create_permission_set(st, "ps");

        let config = json!({
            "AccessControlAttributes": [
                {"Key": "costCenter", "Value": {"Source": ["$.costCenter"]}}
            ]
        });
        handle(
            st,
            &json!({"InstanceArn": &inst, "InstanceAccessControlAttributeConfiguration": config}),
            &req("CreateInstanceAccessControlAttributeConfiguration"),
        );
        // a second create conflicts.
        let dup = handle(
            st,
            &json!({"InstanceArn": &inst, "InstanceAccessControlAttributeConfiguration": config}),
            &req("CreateInstanceAccessControlAttributeConfiguration"),
        );
        assert_eq!(dup.status, 400);
        assert_eq!(body(&dup)["__type"], "ConflictException");

        let described = body(&handle(
            st,
            &json!({"InstanceArn": &inst}),
            &req("DescribeInstanceAccessControlAttributeConfiguration"),
        ));
        assert_eq!(described["Status"], "ENABLED");
        assert_eq!(
            described["InstanceAccessControlAttributeConfiguration"]["AccessControlAttributes"][0]
                ["Key"],
            "costCenter"
        );

        // updates replace the configuration.
        handle(
            st,
            &json!({"InstanceArn": &inst, "InstanceAccessControlAttributeConfiguration": {
                "AccessControlAttributes": []
            }}),
            &req("UpdateInstanceAccessControlAttributeConfiguration"),
        );
        let described = body(&handle(
            st,
            &json!({"InstanceArn": &inst}),
            &req("DescribeInstanceAccessControlAttributeConfiguration"),
        ));
        assert_eq!(
            described["InstanceAccessControlAttributeConfiguration"]["AccessControlAttributes"]
                .as_array()
                .unwrap()
                .len(),
            0
        );

        handle(
            st,
            &json!({"InstanceArn": &inst}),
            &req("DeleteInstanceAccessControlAttributeConfiguration"),
        );
        let missing = handle(
            st,
            &json!({"InstanceArn": &inst}),
            &req("DescribeInstanceAccessControlAttributeConfiguration"),
        );
        assert_eq!(missing.status, 400);
        assert_eq!(body(&missing)["__type"], "ResourceNotFoundException");

        // permissions boundary round-trip on the permission set.
        let boundary = json!({"ManagedPolicyArn": "arn:aws:iam::aws:policy/AdministratorAccess"});
        let missing = body(&handle(
            st,
            &json!({"InstanceArn": &inst, "PermissionSetArn": &ps}),
            &req("GetPermissionsBoundaryForPermissionSet"),
        ));
        assert_eq!(missing["__type"], "ResourceNotFoundException");
        handle(
            st,
            &json!({"InstanceArn": &inst, "PermissionSetArn": &ps, "PermissionsBoundary": boundary}),
            &req("PutPermissionsBoundaryToPermissionSet"),
        );
        let got = body(&handle(
            st,
            &json!({"InstanceArn": &inst, "PermissionSetArn": &ps}),
            &req("GetPermissionsBoundaryForPermissionSet"),
        ));
        assert_eq!(
            got["PermissionsBoundary"]["ManagedPolicyArn"],
            "arn:aws:iam::aws:policy/AdministratorAccess"
        );
        handle(
            st,
            &json!({"InstanceArn": &inst, "PermissionSetArn": &ps}),
            &req("DeletePermissionsBoundaryFromPermissionSet"),
        );
        let missing = handle(
            st,
            &json!({"InstanceArn": &inst, "PermissionSetArn": &ps}),
            &req("GetPermissionsBoundaryForPermissionSet"),
        );
        assert_eq!(body(&missing)["__type"], "ResourceNotFoundException");
    }

    #[test]
    fn operation_status_lists() {
        let st = &mut fresh_state(111_000_000_015);
        let inst = instance_arn(st);
        let ps = create_permission_set(st, "ps");

        let provisioned = body(&handle(
            st,
            &json!({"InstanceArn": &inst, "PermissionSetArn": &ps}),
            &req("ProvisionPermissionSet"),
        ));
        let request_id = provisioned["PermissionSetProvisioningStatus"]["RequestId"]
            .as_str()
            .unwrap()
            .to_string();

        let described = body(&handle(
            st,
            &json!({
                "InstanceArn": &inst,
                "ProvisionPermissionSetRequestId": &request_id,
            }),
            &req("DescribePermissionSetProvisioningStatus"),
        ));
        assert_eq!(
            described["PermissionSetProvisioningStatus"]["Status"],
            "SUCCEEDED"
        );
        assert_eq!(
            described["PermissionSetProvisioningStatus"]["PermissionSetArn"],
            ps
        );

        let listed = body(&handle(
            st,
            &json!({"InstanceArn": &inst}),
            &req("ListPermissionSetProvisioningStatus"),
        ));
        assert_eq!(
            listed["PermissionSetsProvisioningStatus"][0]["RequestId"],
            request_id
        );

        // unknown request ids → the sibling empty-message RNF.
        let missing = handle(
            st,
            &json!({
                "InstanceArn": &inst,
                "ProvisionPermissionSetRequestId": "nope",
            }),
            &req("DescribePermissionSetProvisioningStatus"),
        );
        assert_eq!(missing.status, 400);
        assert_eq!(body(&missing)["__type"], "ResourceNotFoundException");

        // account assignment statuses list by request id.
        let assignment = body(&handle(
            st,
            &json!({
                "InstanceArn": &inst,
                "PermissionSetArn": &ps,
                "PrincipalId": "user-1",
                "PrincipalType": "USER",
                "TargetId": "123456789012",
                "TargetType": "AWS_ACCOUNT",
            }),
            &req("CreateAccountAssignment"),
        ));
        let created_id = assignment["AccountAssignmentCreationStatus"]["RequestId"]
            .as_str()
            .unwrap()
            .to_string();
        let listed = body(&handle(
            st,
            &json!({"InstanceArn": &inst}),
            &req("ListAccountAssignmentCreationStatus"),
        ));
        assert_eq!(
            listed["AccountAssignmentsCreationStatus"][0]["RequestId"],
            created_id
        );
        assert_eq!(
            listed["AccountAssignmentsCreationStatus"][0]["Status"],
            "SUCCEEDED"
        );

        handle(
            st,
            &json!({
                "InstanceArn": &inst,
                "PermissionSetArn": &ps,
                "PrincipalId": "user-1",
                "PrincipalType": "USER",
                "TargetId": "123456789012",
                "TargetType": "AWS_ACCOUNT",
            }),
            &req("DeleteAccountAssignment"),
        );
        let listed = body(&handle(
            st,
            &json!({"InstanceArn": &inst}),
            &req("ListAccountAssignmentDeletionStatus"),
        ));
        assert_eq!(
            listed["AccountAssignmentsDeletionStatus"][0]["RequestId"],
            created_id
        );
    }

    /// moto's `@paginate` semantics: MaxResults slices the list and the
    /// NextToken names the first item AFTER the page (base64 JSON of
    /// `uniqueAttributes`); a bad or unmatched token is moto's
    /// `InvalidToken` 400.
    #[test]
    fn paginated_lists_honor_max_results_and_next_token() {
        let st = &mut fresh_state(111_000_000_016);
        let inst = instance_arn(st);
        let a = create_permission_set(st, "a");
        let b = create_permission_set(st, "b");
        let c = create_permission_set(st, "c");

        let page1 = body(&handle(
            st,
            &json!({"InstanceArn": &inst, "MaxResults": 1}),
            &req("ListPermissionSets"),
        ));
        assert_eq!(page1["PermissionSets"], json!([json!(a)]));
        let token = page1["NextToken"].as_str().unwrap().to_string();

        // The token is base64 of {"uniqueAttributes": "<next arn>"}.
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(token.as_bytes())
            .unwrap();
        let doc: Value = serde_json::from_slice(&decoded).unwrap();
        assert_eq!(doc["uniqueAttributes"], json!(b));

        let page2 = body(&handle(
            st,
            &json!({"InstanceArn": &inst, "MaxResults": 1, "NextToken": &token}),
            &req("ListPermissionSets"),
        ));
        assert_eq!(page2["PermissionSets"], json!([json!(b)]));

        let page3 = body(&handle(
            st,
            &json!({"InstanceArn": &inst, "MaxResults": 1, "NextToken": page2["NextToken"]}),
            &req("ListPermissionSets"),
        ));
        assert_eq!(page3["PermissionSets"], json!([json!(c)]));
        assert!(page3["NextToken"].is_null());

        // A token naming nothing is moto's InvalidToken.
        let bad = handle(
            st,
            &json!({"InstanceArn": &inst, "NextToken": token_for("missing|arn")}),
            &req("ListPermissionSets"),
        );
        assert_eq!(bad.status, 400);
        let e = body(&bad);
        assert_eq!(e["__type"], "InvalidToken");
        assert_eq!(e["message"], "Invalid Token: Resource not found!");

        // A garbage token is also InvalidToken.
        let garbage = handle(
            st,
            &json!({"InstanceArn": &inst, "NextToken": "garbage!!"}),
            &req("ListPermissionSets"),
        );
        let e = body(&garbage);
        assert_eq!(e["__type"], "InvalidToken");

        // A falsy MaxResults falls back to moto's limit_default 100.
        let all = body(&handle(
            st,
            &json!({"InstanceArn": &inst, "MaxResults": 0}),
            &req("ListPermissionSets"),
        ));
        assert_eq!(all["PermissionSets"].as_array().unwrap().len(), 3);
        assert!(all["NextToken"].is_null());

        // The same paginator drives the managed-policy list.
        attach_managed(st, &inst, &a, &policy_arn("AmazonEC2ReadOnlyAccess"));
        attach_managed(st, &inst, &a, &policy_arn("AmazonS3ReadOnlyAccess"));
        let listed = body(&handle(
            st,
            &json!({
                "InstanceArn": &inst,
                "PermissionSetArn": &a,
                "MaxResults": 1,
            }),
            &req("ListManagedPoliciesInPermissionSet"),
        ));
        assert_eq!(
            listed["AttachedManagedPolicies"][0]["Name"],
            "AmazonEC2ReadOnlyAccess"
        );
        assert!(listed["NextToken"].is_string());
    }

    /// ListAccountAssignments filters on the instance ARN too (moto's
    /// three-term filter) and paginates on the four-attribute token.
    #[test]
    fn list_account_assignments_instance_scope_and_pages() {
        let st = &mut fresh_state(111_000_000_016);
        let inst = instance_arn(st);
        let other = format!("{inst}/x");
        let ps = create_permission_set(st, "a");
        for (principal, ps_arn, target) in [
            ("u-1", ps.clone(), "123456789012"),
            ("u-2", ps.clone(), "123456789012"),
            ("u-3", ps.clone(), "123456789012"),
        ] {
            handle(
                st,
                &json!({
                    "InstanceArn": &inst,
                    "PermissionSetArn": ps_arn,
                    "PrincipalId": principal,
                    "PrincipalType": "USER",
                    "TargetId": target,
                    "TargetType": "AWS_ACCOUNT",
                }),
                &req("CreateAccountAssignment"),
            );
        }
        // A different permission set never leaks into ps-a's rows and a
        // foreign instance arn never leaks in either.
        let page = body(&handle(
            st,
            &json!({
                "InstanceArn": &other,
                "AccountId": "123456789012",
                "PermissionSetArn": &ps,
            }),
            &req("ListAccountAssignments"),
        ));
        assert_eq!(page["AccountAssignments"].as_array().unwrap().len(), 0);

        let page1 = body(&handle(
            st,
            &json!({
                "InstanceArn": &inst,
                "AccountId": "123456789012",
                "PermissionSetArn": &ps,
                "MaxResults": 2,
            }),
            &req("ListAccountAssignments"),
        ));
        assert_eq!(page1["AccountAssignments"].as_array().unwrap().len(), 2);
        assert!(page1["NextToken"].is_string());

        let page2 = body(&handle(
            st,
            &json!({
                "InstanceArn": &inst,
                "AccountId": "123456789012",
                "PermissionSetArn": &ps,
                "NextToken": page1["NextToken"],
            }),
            &req("ListAccountAssignments"),
        ));
        assert_eq!(page2["AccountAssignments"].as_array().unwrap().len(), 1);
        assert!(page2["NextToken"].is_null());
    }

    /// moto's four-term filter on ListAccountAssignmentsForPrincipal: the
    /// requested principal, principal type, instance and optional
    /// Filter.AccountId all reduce the output (and nothing leaks across).
    #[test]
    fn list_account_assignments_for_principal_filter() {
        let st = &mut fresh_state(111_000_000_017);
        let inst = instance_arn(st);
        let ps = create_permission_set(st, "a");
        for (pid, ptype, target) in [
            ("u-1", "USER", "111111111111"),
            ("g-1", "GROUP", "111111111111"),
            ("u-1", "USER", "222222222222"),
        ] {
            handle(
                st,
                &json!({
                    "InstanceArn": &inst,
                    "PermissionSetArn": &ps,
                    "PrincipalId": pid,
                    "PrincipalType": ptype,
                    "TargetId": target,
                    "TargetType": "ACCOUNT",
                }),
                &req("CreateAccountAssignment"),
            );
        }

        // A foreign instance yields nothing (no cross-instance leak).
        let foreign = body(&handle(
            st,
            &json!({"InstanceArn": "arn:aws:sso:::instance/ssoins-ghost", "PrincipalId": "u-1", "PrincipalType": "USER"}),
            &req("ListAccountAssignmentsForPrincipal"),
        ));
        assert_eq!(foreign["AccountAssignments"].as_array().unwrap().len(), 0);

        let listed = body(&handle(
            st,
            &json!({"InstanceArn": &inst, "PrincipalId": "u-1", "PrincipalType": "USER"}),
            &req("ListAccountAssignmentsForPrincipal"),
        ));
        let rows = listed["AccountAssignments"].as_array().unwrap();
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|r| r["PrincipalId"] == json!("u-1")));

        // Filter.AccountId must match the target id.
        let filtered = body(&handle(
            st,
            &json!({
                "InstanceArn": &inst,
                "PrincipalId": "u-1",
                "PrincipalType": "USER",
                "Filter": {"AccountId": "222222222222"},
            }),
            &req("ListAccountAssignmentsForPrincipal"),
        ));
        let rows = filtered["AccountAssignments"].as_array().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["AccountId"], "222222222222");

        let mismatched = body(&handle(
            st,
            &json!({
                "InstanceArn": &inst,
                "PrincipalId": "u-1",
                "PrincipalType": "USER",
                "Filter": {"AccountId": "999999999999"},
            }),
            &req("ListAccountAssignmentsForPrincipal"),
        ));
        assert_eq!(
            mismatched["AccountAssignments"].as_array().unwrap().len(),
            0
        );

        // MaxResults walks two pages.
        let page1 = body(&handle(
            st,
            &json!({
                "InstanceArn": &inst,
                "PrincipalId": "u-1",
                "PrincipalType": "USER",
                "MaxResults": 1,
            }),
            &req("ListAccountAssignmentsForPrincipal"),
        ));
        assert_eq!(page1["AccountAssignments"].as_array().unwrap().len(), 1);
        let page2 = body(&handle(
            st,
            &json!({
                "InstanceArn": &inst,
                "PrincipalId": "u-1",
                "PrincipalType": "USER",
                "NextToken": page1["NextToken"],
            }),
            &req("ListAccountAssignmentsForPrincipal"),
        ));
        assert_eq!(page2["AccountAssignments"].as_array().unwrap().len(), 1);
    }

    /// moto reads SessionDuration with a 3600 default (an int) and renders
    /// the provisioning status without a fabricated FailureReason member.
    #[test]
    fn permission_set_default_duration_and_provision_status() {
        let st = &mut fresh_state(111_000_000_018);
        let created = body(&handle(
            st,
            &json!({"InstanceArn": instance_arn(st), "Name": "a"}),
            &req("CreatePermissionSet"),
        ));
        assert_eq!(created["PermissionSet"]["SessionDuration"], 3600);

        let given = body(&handle(
            st,
            &json!({"InstanceArn": instance_arn(st), "Name": "b", "SessionDuration": "PT1H"}),
            &req("CreatePermissionSet"),
        ));
        assert_eq!(given["PermissionSet"]["SessionDuration"], "PT1H");

        let inst = instance_arn(st);
        let ps = created["PermissionSet"]["PermissionSetArn"].clone();
        let provisioned = body(&handle(
            st,
            &json!({"InstanceArn": &inst, "PermissionSetArn": &ps}),
            &req("ProvisionPermissionSet"),
        ));
        let status = &provisioned["PermissionSetProvisioningStatus"];
        assert!(!status.as_object().unwrap().contains_key("FailureReason"));
    }
}
